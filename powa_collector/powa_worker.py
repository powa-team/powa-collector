"""
PowaThread: powa-collector dedicated remote server thread.

One of such thread is started per remote server by the main thred.  Each
threads will use 2 connections:

    - a persistent dedicated connection to the remote server, where it'll get
      the source data
    - a connection to the repository server, to write the source data and
      perform the snapshot.  This connection is created and dropped at each
      checkpoint
"""
from collections import defaultdict
from decimal import Decimal
import threading
import time
import psycopg2
from psycopg2.extras import DictCursor
import logging
import random
from powa_collector.customconn import get_connection
from powa_collector.snapshot import (get_global_snapfuncs_sql, get_src_query,
                                     get_db_mod_snapfuncs_sql,
                                     get_db_cat_snapfuncs_sql,
                                     get_global_tmp_name,
                                     get_nsp, copy_remote_data_to_repo)
from powa_collector.utils import get_powa_version


class PowaThread (threading.Thread):
    """A Powa collector thread. Derives from threading.Thread
    Manages a monitored remote server.
    """
    def __init__(self, name, repository, config, debug):
        """Instance creator. Starts threading and logger"""
        threading.Thread.__init__(self)
        # we use this event to sleep on the worker main loop.  It'll be set by
        # the main thread through one of the public functions, when a SIGHUP
        # was received to notify us to reload our config, or if we should
        # terminate.  All public functions will first set the required event
        # before setting this one, to avoid missing an event in case of the
        # sleep ends at exactly the same time
        self.__stop_sleep = threading.Event()
        # this event is set when we should terminate the thread
        self.__stopping = threading.Event()
        # this event is set when we should reload the configuration
        self.__got_sighup = threading.Event()
        # this event is set when we should force an immediate snapshot
        self.__force_snapshot = threading.Event()
        # this event is set internally while a snapshot is being performed
        self.__snapshot_in_progress = threading.Event()
        # protects __register_cat_refresh_dbnames only
        self.__register_cat_refresh_lock = threading.Lock()
        # Info for registering catalog refresh.  None means no refresh asked,
        # empty array means all db, otherwise a list of dbnames.  There's no
        # associated Event as this won't change the normal snapshot scheduling.
        self.__register_cat_refresh_dbnames = None
        self.__connected = threading.Event()
        # the alias can be the dsn is use_server_alias is false or there's no
        # alias defined for that remote server.
        self.name = config["alias"]
        self.__repository = repository
        self.__config = config
        self.__pending_config = None
        self.__update_dep_versions = False
        self.__remote_conn = None
        self.__repo_conn = None
        self.__last_repo_conn_errored = False
        self.logger = logging.getLogger("powa-collector")
        # last snapshot time, None if unknown
        self.__last_snap_time = None
        self.__debug = debug

        extra = {'threadname': self.name}
        self.logger = logging.LoggerAdapter(self.logger, extra)

        self.logger.debug("Creating worker %s: %r" % (name, config))

    def __repr__(self):
        dsn = self.__config["dsn"].copy()
        if ("password" in dsn):
            dsn["password"] = "<REDACTED>"

        return ("%s: %s" % (self.name, dsn))

    def __maybe_load_powa(self, conn):
        """Loads Powa if it's not already and it's needed.
        Only supports 4.0+ extension, and this version can be loaded on the fly
        """

        ver = get_powa_version(conn)

        if (not ver):
            self.logger.error("PoWA extension not found")
            self.__disconnect_all()
            self.__stopping.set()
            return
        elif (int(ver[0][0]) < 4):
            self.logger.error("Incompatible PoWA version, found %s,"
                              " requires at least 4.0.0" % ver[1])
            self.__disconnect_all()
            self.__stopping.set()
            return

        # make sure the GUC are present in case powa isn't in
        # shared_preload_librairies.  This is only required for powa
        # 4.0.x.
        if (int(ver[0][0]) == 4 and int(ver[0][1]) == 0):
            try:
                cur = conn.cursor()
                cur.execute("LOAD 'powa'")
                cur.close()
                conn.commit()
            except psycopg2.Error as e:
                self.logger.error("Could not load extension powa:\n%s" % e)
                self.__disconnect_all()
                self.__stopping.set()

    def __save_versions(self):
        """Save the versions we collect on the remote server in the repository"""
        srvid = self.__config["srvid"]

        if (self.__repo_conn is None):
            self.__connect()

        ver = get_powa_version(self.__repo_conn)

        # Check and update PG and dependencies versions, for powa 4.1+
        if (not ver or (int(ver[0][0]) == 4 and int(ver[0][1]) == 0)):
            self.__disconnect_repo()
            return

        self.logger.debug("Checking postgres and dependencies versions")

        if (self.__remote_conn is None or self.__repo_conn is None):
            self.logger.error("Could not check PoWA")
            return

        cur = self.__remote_conn.cursor()
        repo_cur = self.__repo_conn.cursor()

        cur.execute("""
                SELECT setting
                FROM pg_settings
                WHERE name = 'server_version'
                --WHERE name = 'server_version_num'
                """)
        server_num = cur.fetchone()
        repo_cur.execute("""
                SELECT version
                FROM {powa}.powa_servers
                WHERE id = %(srvid)s
                """, {'srvid': srvid})
        repo_num = cur.fetchone()

        if (repo_num is None or repo_num[0] != server_num[0]):
            try:
                repo_cur.execute("""
                        UPDATE {powa}.powa_servers
                        SET version = %(version)s
                        WHERE id = %(srvid)s
                        """, {'srvid': srvid, 'version': server_num[0]})
                self.__repo_conn.commit()
            except Exception as e:
                self.logger.warning("Could not save server version"
                                    + ": %s" % (e))
                self.__repo_conn.rollback()

        tbl_config = "powa_extension_config"
        if ((int(ver[0][0]) <= 4)):
            tbl_config = "powa_extensions"

        hypo_ver = None
        repo_cur.execute("""
            SELECT extname, version
            FROM {powa}.""" + tbl_config + """
            WHERE srvid = %(srvid)s
            """ % {'srvid': srvid})
        exts = repo_cur.fetchall()

        for ext in exts:
            if (ext[0] == 'hypopg'):
                hypo_ver = ext[1]

            cur.execute("""
            SELECT extversion
            FROM pg_extension
            WHERE extname = %(extname)s
                    """, {'extname': ext[0]})
            remote_ver = cur.fetchone()

            if (not remote_ver):
                self.logger.debug("No version found for extension "
                                  + "%s on server %d" % (ext[0], srvid))
                continue

            if (ext[1] is None or ext[1] != remote_ver[0]):
                try:
                    repo_cur.execute("""
                            UPDATE {powa}.""" + tbl_config + """
                            SET version = %(version)s
                            WHERE srvid = %(srvid)s
                            AND extname = %(extname)s
                            """, {'version': remote_ver, 'srvid': srvid,
                                  'extname': ext[0]})
                    self.__repo_conn.commit()
                except Exception as e:
                    self.logger.warning("Could not save version for extension "
                                        + "%s: %s" % (ext[0], e))
                    self.__repo_conn.rollback()

        # Special handling of hypopg, which isn't required to be installed in
        # the powa dedicated database.
        cur.execute("""
            SELECT default_version
            FROM pg_available_extensions
            WHERE name = 'hypopg'
        """)
        remote_ver = cur.fetchone()

        if (remote_ver is None):
            try:
                repo_cur.execute("""
                        DELETE FROM {powa}.""" + tbl_config + """
                        WHERE srvid = %(srvid)s
                        AND extname = 'hypopg'
                        """, {'srvid': srvid, 'hypo_ver': remote_ver})
                self.__repo_conn.commit()
            except Exception as e:
                self.logger.warning("Could not save version for extension "
                                    + "hypopg: %s" % (e))
                self.__repo_conn.rollback()
        elif (remote_ver != hypo_ver):
            try:
                if (hypo_ver is None):
                    repo_cur.execute("""
                            INSERT INTO {powa}.""" + tbl_config + """
                                (srvid, extname, version)
                            VALUES (%(srvid)s, 'hypopg', %(hypo_ver)s)
                            """, {'srvid': srvid, 'hypo_ver': remote_ver})
                else:
                    repo_cur.execute("""
                            UPDATE {powa}.""" + tbl_config + """
                            SET version = %(hypo_ver)s
                            WHERE srvid = %(srvid)s
                            AND extname = 'hypopg'
                            """, {'srvid': srvid, 'hypo_ver': remote_ver})

                self.__repo_conn.commit()
            except Exception as e:
                self.logger.warning("Could not save version for extension "
                                    + "hypopg: %s" % (e))
                self.__repo_conn.rollback()

        cur.close()
        self.__remote_conn.commit()
        rep_cur.close()
        self.__disconnect_repo()

    def __check_powa(self):
        """Check that Powa is ready on the remote server."""
        if (self.__remote_conn is None):
            self.__connect()

        if (self.is_stopping()):
            return

        # make sure the GUC are present in case powa isn't in
        # shared_preload_librairies.  This is only required for powa
        # 4.0.x.
        if (self.__remote_conn is not None):
            self.__maybe_load_powa(self.__remote_conn)

        if (self.is_stopping()):
            return

        # Check and update PG and dependencies versions if possible
        self.__save_versions()

    def __reload(self):
        """Reload configuration
        Disconnect from everything, read new configuration, reconnect, update
        dependencies, check Powa is still available The new session could be
        totally different
        """
        self.logger.info("Reloading configuration")
        if (self.__pending_config is not None):
            self.__config = self.__pending_config
            self.__pending_config = None
            self.__disconnect_all()
            self.__connect()
        if (self.__update_dep_versions):
            self.__update_dep_versions = False
            self.__check_powa()
        self.__got_sighup.clear()

    def __report_error(self, msg, replace=True):
        """Store errors in the repository database.
        replace means we overwrite current stored errors in the database for
        this server. Else we append"""
        if (self.__repo_conn is not None):
            if (type(msg).__name__ == 'list'):
                error = msg
            else:
                error = [msg]
            srvid = self.__config["srvid"]
            cur = self.__repo_conn.cursor()
            cur.execute("SAVEPOINT metas")
            try:
                if (replace):
                    cur.execute("""UPDATE {powa}.powa_snapshot_metas
                        SET errors = %s
                        WHERE srvid = %s
                    """, (error, srvid))
                else:
                    cur.execute("""UPDATE {powa}.powa_snapshot_metas
                        SET errors = pg_catalog.array_cat(errors, %s)
                        WHERE srvid = %s
                    """, (error, srvid))
                cur.execute("RELEASE metas")
            except psycopg2.Error as e:
                err = "Could not report error for server %d:\n%s" % (srvid, e)
                self.logger.warning(err)
                cur.execute("ROLLBACK TO metas")
            self.__repo_conn.commit()

    def __connect(self):
        """Connect to a remote server
        Override lock_timeout, application name"""
        if ('dsn' not in self.__repository or 'dsn' not in self.__config):
            self.logger.error("Missing connection info")
            self.__stopping.set()
            return

        try:
            if (self.__repo_conn is None):
                self.logger.debug("Connecting on repository...")
                self.__repo_conn = get_connection(self.logger,
                                                  self.__debug,
                                                  self.__repository['dsn'])
                self.logger.debug("Connected.")
                # make sure the GUC are present in case powa isn't in
                # shared_preload_librairies.  This is only required for powa
                # 4.0.x.
                self.__maybe_load_powa(self.__repo_conn)

                # Return now if __maybe_load_powa asked to stop
                if (self.is_stopping()):
                    return

                cur = self.__repo_conn.cursor()
                cur.execute("""SELECT
                    pg_catalog.set_config(name, '2000', false)
                    FROM pg_catalog.pg_settings
                    WHERE name = 'lock_timeout'
                    AND setting = '0'""")

                cur.execute("SET application_name = %s",
                            ('PoWA collector - repo_conn for worker ' + self.name,))
                cur.close()
                self.__repo_conn.commit()
                self.__last_repo_conn_errored = False

            if (self.__remote_conn is None):
                self.logger.debug("Connecting on remote database...")
                self.__remote_conn = get_connection(self.logger,
                                                    self.__debug,
                                                    **self.__config['dsn'])
                self.logger.debug("Connected.")

                # make sure the GUC are present in case powa isn't in
                # shared_preload_librairies.  This is only required for powa
                # 4.0.x.
                if (self.__remote_conn is not None):
                    self.__maybe_load_powa(self.__remote_conn)

                # Return now if __maybe_load_powa asked to stop
                if (self.is_stopping()):
                    return

                cur = self.__remote_conn.cursor()
                cur.execute("""SELECT
                    pg_catalog.set_config(name, '2000', false)
                    FROM pg_catalog.pg_settings
                    WHERE name = 'lock_timeout'
                    AND setting = '0'""")
                cur.execute("SET application_name = %s",
                            ('PoWA collector - worker ' + self.name,))
                cur.close()
                self.__remote_conn.commit()

                self.__connected.set()
        except psycopg2.Error as e:
            self.logger.error("Error connecting on %s:\n%s" %
                              (self.__config["dsn"], e))

            if (self.__repo_conn is not None):
                self.__report_error("%s" % (e))
            else:
                self.__last_repo_conn_errored = True

    def __disconnect_all(self):
        """Disconnect from remote server and repository server"""
        if (self.__remote_conn is not None):
            self.logger.info("Disconnecting from remote server")
            self.__remote_conn.close()
            self.__remote_conn = None
        if (self.__repo_conn is not None):
            self.logger.info("Disconnecting from repository")
            self.__disconnect_repo()
        self.__connected.clear()

    def __disconnect_repo(self):
        """Disconnect from repo"""
        if (self.__repo_conn is not None):
            self.__repo_conn.close()
            self.__repo_conn = None

    def __disconnect_all_and_exit(self):
        """Disconnect all and stop the thread"""
        # this is the exit point
        self.__disconnect_all()
        self.logger.info("stopped")
        self.__stopping.clear()

    def __worker_main(self):
        """The thread's main loop
        Get latest snapshot timestamp for the remote server and determine how
        long to sleep before performing the next snapshot.
        Add a random seed to avoid doing all remote servers simultaneously"""
        self.__last_snap_time = None
        self.__check_powa()

        # __check_powa() is only responsible for making sure that the remote
        # connection is opened.
        if (self.__repo_conn is None):
            self.__connect()

        # if this worker has been restarted, restore the previous snapshot
        # time to try to keep up on the same frequency
        if (not self.is_stopping() and self.__repo_conn is not None):
            cur = None
            try:
                cur = self.__repo_conn.cursor()
                cur.execute("""SELECT EXTRACT(EPOCH FROM snapts)
                    FROM {powa}.powa_snapshot_metas
                    WHERE srvid = %d
                    """ % self.__config["srvid"])
                row = cur.fetchone()
                if not row:
                    self.logger.error("Server %d was not correctly registered"
                                      " (no powa_snapshot_metas record)"
                                      % self.__config["srvid"])
                    self.logger.debug("Server configuration details:\n%r"
                                      % self.__config)
                    self.logger.error("Stopping worker for server %d"
                                      % self.__config["srvid"])
                    self.__stopping.set()
                if row:
                    self.__last_snap_time = float(row[0])
                    self.logger.debug("Retrieved last snapshot time:"
                                      + " %r" % self.__last_snap_time)
                cur.close()
                self.__repo_conn.commit()
            except Exception as e:
                self.logger.warning("Could not retrieve last snapshot"
                                    + " time: %s" % (e))
                if (cur is not None):
                    cur.close()
                self.__repo_conn.rollback()

        # Normalize unknkown last snapshot time
        if (self.__last_snap_time == Decimal('-Infinity')):
            self.__last_snap_time = None

        # if this worker was stopped longer than the configured frequency,
        # assign last snapshot time to a random time between now and now minus
        # duration.  This will help to spread the snapshots and avoid activity
        # spikes if the collector itself was stopped for a long time, or if a
        # lot of new servers were added
        freq = self.__config["frequency"]
        if (not self.is_stopping()
            and (
                self.__last_snap_time is None
                or
                ((time.time() - self.__last_snap_time) > freq)
        )):
            random.seed()
            r = random.randint(0, self.__config["frequency"] - 1)
            self.logger.debug("Spreading snapshot: setting last snapshot to"
                              + " %d seconds ago (frequency: %d)" %
                              (r, freq))
            self.__last_snap_time = time.time() - r

        while (not self.is_stopping()):
            if (self.__got_sighup.isSet()):
                self.__reload()

            if ((self.__last_snap_time is None) or
                ((time.time() - self.__last_snap_time) >= freq) or
                (self.__force_snapshot.isSet())):
                try:
                    self.__snapshot_in_progress.set()
                    if (self.__force_snapshot.isSet()):
                        self.__force_snapshot.clear()

                    self.__last_snap_time = time.time()

                    self.__take_snapshot()
                    self.__snapshot_in_progress.clear()
                except psycopg2.Error as e:
                    self.logger.error("Error during snapshot: %s" % e)
                    # It will reconnect automatically at next snapshot
                    self.__disconnect_all()

            time_to_sleep = max(self.__config["frequency"] - \
                                (time.time() - self.__last_snap_time), 0)

            # sleep until the scheduled processing time, or if the main thread
            # asked us to perform an action or if we were asked to stop.
            if (time_to_sleep > 0 and not self.is_stopping()):
                self.__stop_sleep.wait(time_to_sleep)

            # clear the event if it has been set.  We'll process all possible
            # event triggered by it within the next iteration
            if (self.__stop_sleep.isSet()):
                self.__stop_sleep.clear()

        # main loop is over, disconnect and quit
        self.__disconnect_all_and_exit()

    def __get_global_snapfuncs(self, powa_ver):
        """
        Get the list of global snapshot functions (in the dedicated powa
        database), and their associated query_src
        """
        srvid = self.__config["srvid"]

        cur = self.__repo_conn.cursor(cursor_factory=DictCursor)
        cur.execute("SAVEPOINT snapshots")
        try:
            cur.execute(get_global_snapfuncs_sql(powa_ver), (srvid,))
            snapfuncs = cur.fetchall()
            cur.execute("RELEASE snapshots")
        except psycopg2.Error as e:
            cur.execute("ROLLBACK TO snapshots")
            err = "Error while getting snapshot functions:\n%s" % (e)
            self.logger.error(err)
            self.logger.error("Exiting worker for server %s..." % srvid)
            self.__stopping.set()
            return None
        cur.close()

        if (not snapfuncs):
            self.logger.info("No datasource configured for server %d" % srvid)
            self.logger.debug("Committing transaction")
            self.__repo_conn.commit()
            self.__disconnect_repo()
            return None

        return snapfuncs

    def __get_global_src_data(self, powa_ver, ins):
        """
        Retrieve the source global data (in the powa database) from the foreign
        server, and insert them in the *_src_tmp tables on the repository
        server.
        """
        srvid = self.__config["srvid"]
        errors = []

        snapfuncs = self.__get_global_snapfuncs(powa_ver)
        if not snapfuncs:
            # __get_global_snapfuncs already took care of reporting errors
            return errors

        data_src = self.__remote_conn.cursor()

        for snapfunc in snapfuncs:
            if (self.is_stopping()):
                return errors

            kind_name = snapfunc["name"]
            query_source = snapfunc["query_source"]
            cleanup_sql = snapfunc["query_cleanup"]
            function_name = snapfunc["function_name"]
            external = snapfunc["external"]

            self.logger.debug("Working on %s", kind_name)

            # get the SQL needed to insert the query_src data on the remote
            # server into the transient unlogged table on the repository server
            if (query_source is None):
                self.logger.warning("No query_source for %s" % function_name)
                continue

            # get the extension schema on the remote server, or skip this
            # extension if it's not installed reporting an appropriate error
            r_nsp = get_nsp(self.__remote_conn, external, kind_name)
            if r_nsp is None:
                errors.append("Extension %s is not installed on the the "
                              "remote server %s" % (kind_name, self.name))
                continue

            # get the extension schema on the repository server, or skip this
            # extension if it's not installed reporting an appropriate error
            tbl_nsp = get_nsp(self.__repo_conn, external, kind_name)
            if tbl_nsp is None:
                errors.append("Extension %s is not installed on the the "
                              "repository server" % (kind_name, ))
                continue

            # execute the query_src functions on the remote server to get its
            # local data (srvid 0)
            self.logger.debug("Calling %s.%s(0)..." % (r_nsp, query_source))
            data_src_sql = get_src_query(r_nsp, query_source, srvid)

            target_tbl_name = get_global_tmp_name(tbl_nsp, query_source)

            errors.extend(copy_remote_data_to_repo(self, kind_name, data_src,
                                                   data_src_sql, ins,
                                                   target_tbl_name,
                                                   cleanup_sql))

        data_src.close()
        return errors

    def __get_db_mod_snapfuncs(self, srvid):
        """
        Get the list of per-db module, with their associated query_source and
        dbnames.
        """
        server_version_num = self.__remote_conn.server_version

        db_mod_queries = defaultdict(list)

        cur = self.__repo_conn.cursor(cursor_factory=DictCursor)
        cur.execute("SAVEPOINT db_snapshots")
        try:
            cur.execute(get_db_mod_snapfuncs_sql(srvid, server_version_num))
            mod_snapfuncs = cur.fetchall()
            cur.execute("RELEASE db_snapshots")
        except psycopg2.Error as e:
            cur.execute("ROLLBACK TO db_snapshots")
            err = "Error while getting db module snapshot functions:\n%s" % (e)
            self.logger.error(err)
            self.logger.error("Exiting worker for server %s..." % srvid)
            self.__stopping.set()
            return None
        cur.close()

        for func in mod_snapfuncs:
            row = (func['db_module'], func['query_source'], func['tmp_table'])

            if (func['dbnames'] is None):
                db_mod_queries[None].append(row)
            else:
                for dbname in func['dbnames']:
                    db_mod_queries[dbname].append(row)

        return db_mod_queries

    def __get_db_cat_snapfuncs(self, srvid):
        """
        Get the list of per-db catalogs, with their associated query_source and
        dbnames.
        """
        server_version_num = self.__remote_conn.server_version

        cat_queries = []

        cur = self.__repo_conn.cursor(cursor_factory=DictCursor)
        cur.execute("SAVEPOINT db_catalog")
        forced_dbnames = None
        try:
            # Make a copy of the catalog refresh registration if any while
            # holding a lock, and also clear the registration list at the same
            # time.
            # It means that any cat refresh registered after that point won't
            # be lost and will be treated once during the next snapshot.
            with self.__register_cat_refresh_lock:
                if (self.__register_cat_refresh_dbnames is not None):
                    forced_dbnames = self.__register_cat_refresh_dbnames.copy()
                    self.__register_cat_refresh_dbnames = None

            force = True if forced_dbnames is not None else False
            cur.execute(get_db_cat_snapfuncs_sql(srvid, server_version_num,
                                                 force))
            cat_snapfuncs = cur.fetchall()
            cur.execute("RELEASE db_catalog")
        except psycopg2.Error as e:
            cur.execute("ROLLBACK TO db_catalog")
            err = "Error while getting db catalog snapshot functions:\n%s" % (e)
            self.logger.error(err)
            self.logger.error("Exiting worker for server %s..." % srvid)
            self.__stopping.set()
            return None

        for func in cat_snapfuncs:
            row = (func['catname'], func['query_source'], func['tmp_table'],
                   func['excluded_dbnames'])
            cat_queries.append(row)

        return (cat_queries, forced_dbnames)

    def __get_db_src_data(self, powa_ver, now, ins):
        """
        Retrieve the source per-database data from the foreign server, and
        insert them in the *_src_tmp tables on the repository server.
        This handles both db_modules and catalog datasources.
        """
        srvid = self.__config["srvid"]
        errors = []

        # This is a powa 5+ feature
        if (int(powa_ver[0][0]) < 5):
            return errors

        dbnames = self.__get_remote_dbnames()

        db_mod_queries = self.__get_db_mod_snapfuncs(srvid)
        (db_cat_queries, forced_cat_dbnames) = self.__get_db_cat_snapfuncs(srvid)
        for dbname in dbnames:
            # Skip that database if no module configured for it
            do_db_module = (None in db_mod_queries or dbname in db_mod_queries)
            do_db_cat = False

            if (forced_cat_dbnames is not None):
                if (len(forced_cat_dbnames) == 0):
                    do_db_cat = True
                else:
                    do_db_cat = (dbname in forced_cat_dbnames)

            if not do_db_cat:
                for (_, _, _, excluded_dbnames) in db_cat_queries:
                    if (dbname not in excluded_dbnames):
                        do_db_cat = True
                        break

            # Skip this database if there's no db_module or catalog to import
            if (not do_db_module and not do_db_cat):
                continue

            self.logger.debug("Working on remote database %s", dbname)
            errors.extend(self.__get_db_src_data_onedb(now, dbname, ins,
                                                       db_mod_queries,
                                                       db_cat_queries,
                                                       forced_cat_dbnames))
            if (self.is_stopping()):
                if (len(errors) > 0):
                    self.__report_error(errors)
                return []

        return errors

    def __get_db_src_data_onedb(self, now, dbname, ins, db_mod_queries,
                                db_cat_queries, forced_cat_dbnames):
        """
        Per-database worker function for __get_db_src_data(), taking care of
        db modules and catalog import.
        """
        srvid = self.__config["srvid"]
        errors = []

        self.logger.debug("Working on remote database %s", dbname)

        try:
            dbconn = get_connection(self.logger,
                                    self.__debug,
                                    override_dbname=dbname,
                                    **self.__config['dsn'])
        except psycopg2.Error as e:
            err = "Could not connect to remote database %s:\n%s" % (dbname, e)
            self.logger.warning(err)
            errors.append(err)
            return errors

        data_src = dbconn.cursor()

        # first, process the enabled db_modules on that database
        for row in (db_mod_queries.get(None, []) + \
                    db_mod_queries.get(dbname, [])):
            if (self.is_stopping()):
                dbconn.close()
                return errors

            (db_module, query_source, tmp_table) = row

            data_src_sql = """SELECT %d AS srvid, '%s' AS ts, d.dbid, src.*
                FROM (%s) src
                CROSS JOIN (
                    SELECT oid AS dbid
                    FROM pg_catalog.pg_database
                    WHERE datname = current_database()
                ) d""" % (srvid, now, query_source)
            self.logger.debug("Db module %s, calling SQL:\n%s" % (db_module,
                                                                 data_src_sql))

            errors.extend(copy_remote_data_to_repo(self, db_module, data_src,
                                                   data_src_sql, ins,
                                                   tmp_table))

        # then process the outdated catalogs on that databasr
        for row in db_cat_queries:
            (catname, query_source, tmp_table, excluded_dbnames) = row

            # Ignore this catalog if excluded for this database
            if (forced_cat_dbnames is not None):
                if (len(forced_cat_dbnames) == 0):
                    # empty means to process all databases
                    pass
                elif (dbname not in forced_cat_dbnames):
                    continue
            if (dbname in excluded_dbnames):
                continue

            data_src_sql = """SELECT %d AS srvid, d.dbid, src.*
                FROM (%s) src
                CROSS JOIN (
                    SELECT oid AS dbid
                    FROM pg_catalog.pg_database
                    WHERE datname = current_database()
                ) d""" % (srvid, query_source)
            self.logger.debug("Catalog %s, calling SQL:\n:%s" % (catname,
                                                                 data_src_sql))

            errors.extend(copy_remote_data_to_repo(self, catname, data_src,
                                                   data_src_sql, ins,
                                                   tmp_table))

        data_src.close()
        dbconn.close()

        return errors

    def __get_remote_dbnames(self):
        """
        Get the list of databases on the remote servers
        """
        res = []
        cur = self.__remote_conn.cursor()
        cur.execute("""SELECT datname
            FROM pg_catalog.pg_database
            WHERE datallowconn""")

        for row in cur.fetchall():
            res.append(row[0])

        cur.close()
        return res

    def __take_snapshot(self):
        """Main part of the worker thread.  This function will call all the
        query_src functions enabled for the target server, and insert all the
        retrieved rows on the repository server, in unlogged tables, and
        finally call powa_take_snapshot() on the repository server to finish
        the distant snapshot.  All is done in one transaction, so that there
        won't be concurrency issues if a snapshot takes longer than the
        specified interval.  This also ensure that all rows will see the same
        snapshot timestamp.
        """
        srvid = self.__config["srvid"]

        if (self.is_stopping()):
            return

        self.__connect()

        if (self.__remote_conn is None):
            self.logger.error("No connection to remote server, snapshot skipped")
            return

        if (self.__repo_conn is None):
            self.logger.error("No connection to repository server, snapshot skipped")
            return

        powa_ver = get_powa_version(self.__repo_conn)
        powa_remote_ver = get_powa_version(self.__remote_conn)

        # We require the same powa major version (X.Y) on the repository and
        # each remote server.
        if (powa_ver[0][0] != powa_remote_ver[0][0] or
            powa_ver[0][1] != powa_remote_ver[0][1]):
            error = ("Incompatible PoWA version between the repository server"
            + " (%s.X) and the remote host (%s.X)" % (
                    powa_ver[0][0] + "." + powa_ver[0][1],
                    powa_remote_ver[0][0] + "." + powa_remote_ver[0][1]))

            self.__report_error(error)
            self.__disconnect_repo()
            return
        ins = self.__repo_conn.cursor()

        # Retrieve the global data from the remote server
        errors = self.__get_global_src_data(powa_ver, ins)
        if (self.is_stopping()):
            if (len(errors) > 0):
                self.__report_error(errors)
            return

        with self.__remote_conn.cursor() as cur:
            cur.execute("SELECT now()")
            now = cur.fetchone()[0]

        # Retrieve the per-db data from the remote server
        errors.extend(self.__get_db_src_data(powa_ver, now, ins))
        if (self.is_stopping()):
            if (len(errors) > 0):
                self.__report_error(errors)
            return

        # call powa_take_snapshot() for the given server
        self.logger.debug("Calling powa_take_snapshot(%d)..." % (srvid))
        sql = ("SELECT {powa}.powa_take_snapshot(%(srvid)d)" % {'srvid': srvid})
        try:
            ins.execute("SAVEPOINT powa_take_snapshot")
            ins.execute(sql)
            val = ins.fetchone()[0]
            if (val != 0):
                self.logger.warning("Number of errors during snapshot: %d",
                                    val)
                self.logger.warning(" Check the logs on the repository server")
            ins.execute("RELEASE powa_take_snapshot")
        except psycopg2.Error as e:
            err = "Error while taking snapshot for server %d:\n%s" % (srvid, e)
            self.logger.warning(err)
            errors.append(err)
            ins.execute("ROLLBACK TO powa_take_snapshot")
            # Manually reset existing errors as powa_take_snapshot, which
            # should be responsible for it, failed
            ins.execute("""UPDATE {powa}.powa_snapshot_metas
                SET errors = NULL
                WHERE srvid = %(srvid)s""", {'srvid': srvid})

        ins.execute("SET application_name = %s",
                    ('PoWA collector - repo_conn for worker ' + self.name,))
        ins.close()

        # we need to report and append errors after calling powa_take_snapshot,
        # since this function will first reset errors
        if (len(errors) > 0):
            self.__report_error(errors, False)

        # and finally commit the transaction
        self.logger.debug("Committing transaction")
        self.__repo_conn.commit()
        self.__remote_conn.commit()

        self.__disconnect_repo()

    def is_stopping(self):
        """Is the thread currently stopping"""
        return self.__stopping.isSet()

    def get_config(self):
        """Returns the thread's config"""
        return self.__config

    def ask_to_stop(self):
        """Ask the thread to stop"""
        self.__stopping.set()
        self.logger.info("Asked to stop...")
        self.__stop_sleep.set()

    def run(self):
        """Start the main loop of the thread"""
        if (not self.is_stopping()):
            self.logger.info("Starting worker")
            self.__worker_main()

    def ask_reload(self, new_config):
        """Ask the thread to reload"""
        self.logger.debug("Reload asked")
        self.__pending_config = new_config
        self.__got_sighup.set()
        self.__stop_sleep.set()

    def ask_update_dep_versions(self):
        """Ask the thread to recompute its dependencies"""
        self.logger.debug("Version dependencies reload asked")
        self.__update_dep_versions = True
        self.__got_sighup.set()
        self.__stop_sleep.set()

    def get_status(self):
        """Get the status: ok, not connected to repo, or not connected to remote"""
        if (self.__repo_conn is None and self.__last_repo_conn_errored):
            return "no connection to repository server"
        if (self.__remote_conn is None):
            return "no connection to remote server"
        else:
            return "running"

    def is_snapshot_in_progress(self):
        """Returns whether the worker is currently performing a snapshot"""
        return self.__snapshot_in_progress.isSet()

    def request_forced_snapshot(self):
        """Ask for an immediate snapshot"""
        self.logger.debug('Forced snapshot required')

        if (self.__snapshot_in_progress.isSet()):
            return ('ERROR', 'A snapshot is already in progress')

        if (self.__force_snapshot.isSet()):
            return ('ERROR', 'A forced snapshot is already scheduled')

        if ((time.time() - self.__last_snap_time ) < 10):
            return ('ERROR', 'Last snapshot was less than 10s. ago')

        self.__force_snapshot.set()
        self.__stop_sleep.set()
        return ('OK', '')

    def register_cat_refresh(self, dbnames=[]):
        """
        Register a catalog refresh on the wanted databases (or all) during the
        next snapshot.
        """
        self.logger.debug('Cat refresh required on databases %r', dbnames)

        with self.__register_cat_refresh_lock:
            if (self.__register_cat_refresh_dbnames is None):
                self.__register_cat_refresh_dbnames = dbnames.copy()
            elif (len(dbnames) == 0):
                # passed empty array means all databases
                self.__register_cat_refresh_dbnames = []
            # If the stored array is empty, it means someone asked to refresh
            # the catalogs on all databases, so nothing to do.  Otherwise, just
            # add the givend databases
            elif (len(self.__register_cat_refresh_dbnames) != 0):
                # it's ok to have duplicates, we don't expect too many request,
                # and we want to minimize the lock time
                self.__register_snapshot.extend(dbnames)

        return ('OK', 'Catalogs will be refreshed during the next snapshot')
