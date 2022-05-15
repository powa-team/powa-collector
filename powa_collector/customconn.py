import psycopg2
from psycopg2.extensions import connection as _connection, cursor as _cursor
from psycopg2.extras import DictCursor
import time


class CustomConnection(_connection):
    """
    Custom psycopg2 connection class that takes care of expanding extension
    schema and optionally logs various information at debug level, both on
    successful execution and in case of error.

    Supports either plain cursor (through CustomCursor) or DictCursor
    (through CustomDictCursor).

    Before execution, and if a _nsps object is found cached in the connection,
    the query will be formatted using its _nsps dict, which contains a list of
    extension_name -> escaped_schema_name mapping.
    All you need to do is pass query strings of the form
    SELECT ... FROM {extension_name}.some_relation ...
    """
    def initialize(self, logger, debug):
        self._logger = logger
        self._debug = debug

    def cursor(self, *args, **kwargs):
        factory = kwargs.get('cursor_factory')

        if factory is None:
            kwargs['cursor_factory'] = CustomCursor
        elif factory == DictCursor:
            kwargs['cursor_factory'] = CustomDictCursor
        else:
            msg = "Unsupported cursor_factory: %s" % factory.__name__
            self._logger.error(msg)
            raise Exception(msg)

        return _connection.cursor(self, *args, **kwargs)


class CustomDictCursor(DictCursor):
    def execute(self, query, params=None):
        query = resolve_nsps(query, self.connection)

        self.timestamp = time.time()
        try:
            return super(CustomDictCursor, self).execute(query, params)
        except Exception as e:
            log_query(self, query, params, e)
            raise e
        finally:
            log_query(self, query, params)


class CustomCursor(_cursor):
    def execute(self, query, params=None):
        query = resolve_nsps(query, self.connection)

        self.timestamp = time.time()
        try:
            return super(CustomCursor, self).execute(query, params)
        except Exception as e:
            log_query(self, query, params, e)
            raise e
        finally:
            log_query(self, query, params)


def resolve_nsps(query, connection):
    if hasattr(connection, '_nsps'):
        return query.format(**connection._nsps)

    return query


def log_query(cls, query, params=None, exception=None):
    t = round((time.time() - cls.timestamp) * 1000, 2)

    fmt = ''
    if exception is not None:
        fmt = "Error during query execution:\n{}\n".format(exception)

    fmt += "query: {ms} ms\n{query}"
    if params is not None:
        fmt += "\n{params}"

    cls.connection._logger.debug(fmt.format(ms=t, query=query, params=params))


def get_connection(logger, debug, *args, **kwargs):
    kwargs['connection_factory'] = CustomConnection
    conn = psycopg2.connect(*args, **kwargs)
    conn.initialize(logger, debug)

    cur = conn.cursor()

    # retrieve and cache the quoted schema for all installed extensions
    logger.debug("Retrieving extension schemas...")
    cur.execute("""SELECT extname, quote_ident(nspname) AS nsp
    FROM pg_catalog.pg_extension e
    JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace""")
    ext_nsps = {row[0]: row[1] for row in cur.fetchall()}
    logger.debug("extension schemas: %r" % ext_nsps)
    conn._nsps = ext_nsps

    cur.close()
    conn.commit()

    return conn
