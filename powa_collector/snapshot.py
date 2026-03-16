from os import SEEK_SET
import psycopg2
import sys
if (sys.version_info < (3, 0)):
    from StringIO import StringIO
else:
    from io import StringIO


def get_global_snapfuncs_sql(ver):
    """Get the list of enabled global functions for snapshotting"""
    # XXX should we ignore entries without query_src?

    if (int(ver[0][0]) >= 5):
        return """SELECT name, external, query_source, query_cleanup,
                    function_name
                  FROM {powa}.powa_functions pf
                  -- FIXME
                  -- JOIN pg_extension ext ON ext.extname = pf.module
                  -- JOIN pg_namespace nsp ON nsp.oid = ext.extnamespace
                  WHERE operation = 'snapshot' AND enabled
                  AND srvid = %s
                  ORDER BY priority"""
    else:
        return """SELECT module AS name, false AS external, query_source,
                    query_cleanup, function_name
                  FROM {powa}.powa_functions pf
                  -- FIXME
                  -- JOIN pg_extension ext ON ext.extname = pf.module
                  -- JOIN pg_namespace nsp ON nsp.oid = ext.extnamespace
                  WHERE operation = 'snapshot' AND enabled
                  AND srvid = %s
                  ORDER BY priority"""


def get_src_query(schema, src_fct, srvid):
    """
    Get the SQL query for global dataousource we'll use to get results from
    a snapshot function
    """
    return ("SELECT %(srvid)d, * FROM %(schema)s.%(fname)s(0)" %
            {'fname': src_fct, 'schema': schema, 'srvid': srvid})


def get_db_mod_snapfuncs_sql(srvid, server_version_num):
    """
    Get the SQL query for a db_module we'll use to get results from a snapshot
    function
    """
    return ("""SELECT db_module, query_source, tmp_table, dbnames
            FROM {powa}.powa_db_functions(%(srvid)d, %(server_version_num)d)
            WHERE operation = 'snapshot'
            ORDER BY priority
            """ % {'srvid': srvid, 'server_version_num': server_version_num})


def get_db_cat_snapfuncs_sql(srvid, server_version_num, force=False):
    """
    Get the SQL query for a db catalog we'll use to get results from a snapshot
    function
    """
    if force:
        interval = 'NULL'
    else:
        interval = "'1 year'"

    return ("""SELECT catname, query_source, tmp_table, excluded_dbnames
            FROM {powa}.powa_catalog_functions(
                %(srvid)d, %(server_version_num)d, %(interval)s
            )""" % {'srvid': srvid,
                    'server_version_num': server_version_num,
                    'interval': interval})


def get_global_tmp_name(schema, src_fct):
    """Get the temp table name we'll use to spool changes"""
    return "%s.%s_tmp" % (schema, src_fct)


def get_nsp(conn, external, module):
    """
    Returns the given module schema.
    If "external" is true, then it's an external extension so returns its
    associated schema.
    If "external" is false then it's an internal module so return the powa
    extension schema instead.

    Returns None if the module is not found.  Caller is responsible for
    reporting an appropriate error in that case.
    """
    if external:
        return conn._nsps.get(module, None)
    else:
        return conn._nsps.get('powa', None)

def get_table_basename(full_table_name, dbcursor):
    """
    Get the basename of a table. We ask PostgreSQL instead of risking doing
    regexp stuff on this
    """
    dbcursor.execute("""SELECT relname
                    FROM pg_class
                    WHERE oid = %s::regclass""" , (full_table_name,))
    row = dbcursor.fetchone()
    if not row:
        return None
    else:
        return row[0]

def copy_remote_data_to_repo(cls, data_name,
                        data_src, data_src_sql, data_ins, target_tbl_name,
                        srvid, cleanup_sql=None):
    """
    Retrieve the wanted datasource from the given connection and insert it on
    the repository server in the given table.

    data_src: the cursor to use to get the data on the remote server
    data_src_sql: the SQL query to execute to get the datasource data
    data_name: a string describing the datasource
    data_ins: the cursor to use to write the data on the repository server
    target_tbl_name: a string containing the (fully qualified and properly
                     quoted) target table name to COPY data to on the
                     repository server
    cleanup_sql: an optional SQL query to execute after executing data_src_sql.
                 Note that this query is executed even if there's an error
                 during data_src_sql execution
    """
    errors = []
    src_ok = True

    # use savepoint, there could be an error while retrieving data on the
    # remote server.
    data_src.execute("SAVEPOINT src")

    # XXX should we use os.pipe() or a temp file instead, to avoid too
    # much memory consumption?
    buf = StringIO()
    try:
        data_src.copy_expert("COPY (%s) TO stdout" % data_src_sql, buf)
        data_src.execute("RELEASE src")
    except psycopg2.Error as e:
        src_ok = False
        err = "Error retrieving datasource data %s:\n%s" % (data_name, e)
        errors.append(err)
        data_src.execute("ROLLBACK TO src")

    # execute the cleanup query if provided
    if (cleanup_sql is not None):
        data_src.execute("SAVEPOINT src")
        try:
            cls.logger.debug("Calling %s..." % cleanup_sql)
            data_src.execute(cleanup_sql)
            data_src.execute("RELEASE src")
        except psycopg2.Error as e:
            err = "Error while calling %s:\n%s" % (cleanup_sql, e)
            errors.append(err)
            data_src.execute("ROLLBACK TO src")

    # If user want to stop the collector or if any error happened during
    # datasource retrieval, there's nothing more to do so simply inform caller
    # of the errors.
    if (cls.is_stopping() or not src_ok):
        return errors

    # insert the data to the local temp table
    temp_table_name = get_table_basename(target_tbl_name, data_ins) + '_' + str(srvid)
    data_ins.execute("CREATE TEMP TABLE IF NOT EXISTS %s (CHECK (srvid = %s)) INHERITS (%s)" % (temp_table_name, srvid, target_tbl_name)) ;
    # Get rid of the lock. We didn't do anything apart from creating the temp table on this session
    # We're doin a COMMIT and a BEGIN, the psycopg2 is useless for savepoints anyway...
    data_ins.execute("COMMIT");
    data_ins.execute("BEGIN");
    data_ins.execute("SAVEPOINT data")
    buf.seek(0, SEEK_SET)
    try:
        # For data import the schema is now on the repository server
        data_ins.copy_expert("COPY %s FROM stdin" % temp_table_name, buf)
        data_ins.execute("RELEASE data")
    except psycopg2.Error as e:
        err = "Error while inserting data:\n%s" % e
        cls.logger.warning(err)
        errors.append(err)
        cls.logger.warning("Giving up for datasource %s", data_name)
        data_ins.execute("ROLLBACK TO data")

    buf.close()

    return errors
