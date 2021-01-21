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
import threading
import time
import calendar
import psycopg2
from psycopg2.extras import DictCursor
import logging
from os import SEEK_SET
import random
import sys
from powa_collector.snapshot import (get_snapshot_functions, get_src_query,
                                     get_tmp_name)
if (sys.version_info < (3, 0)):
    from StringIO import StringIO
else:
    from io import StringIO


class PowaThread (threading.Thread):
    def __init__(self, name, repository, config):
        threading.Thread.__init__(self)
        # we use this event to sleep on the worker main loop.  It'll be set by
        # the main thread through one of the public functions, when a SIGHUP
        # was received to notify us to reload our config, or if we should
        # terminate.  Both public functions will first set the required event
        # before setting this one, to avoid missing an event in case of the
        # sleep ends at exactly the same time
        self.__stop_sleep = threading.Event()
        # this event is set when we should terminate the thread
        self.__stopping = threading.Event()
        # this event is set when we should reload the configuration
        self.__got_sighup = threading.Event()
        self.__connected = threading.Event()
        self.name = name
        self.__repository = repository
        self.__config = config
        self.__pending_config = None
        self.__update_dep_versions = False
        self.__remote_conn = None
        self.__repo_conn = None
        self.__last_repo_conn_errored = False
        self.logger = logging.getLogger("powa-collector")
        self.last_time = None

        extra = {'threadname': self.name}
        self.logger = logging.LoggerAdapter(self.logger, extra)

        self.logger.debug("Creating worker %s: %r" % (name, config))

    def __repr__(self):
        return ("%s: %s" % (self.name, self.__config["dsn"]))

    def __get_powa_version(self, conn):
        cur = conn.cursor()
        cur.execute("""SELECT
            regexp_split_to_array(extversion, '\\.'),
            extversion
            FROM pg_catalog.pg_extension
            WHERE extname = 'powa'""")
        res = cur.fetchone()
        cur.close()

        return res

    def __maybe_load_powa(self, conn):
        ver = self.__get_powa_version(conn)

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
        srvid = self.__config["srvid"]

        if (self.__repo_conn is None):
            self.__connect()

        ver = self.__get_powa_version(self.__repo_conn)

        # Check and update PG and dependencies versions, for powa 4.1+
        if (not ver or (int(ver[0][0]) == 4 and int(ver[0][1]) == 0)):
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
                FROM powa_servers
                WHERE id = %(srvid)s
                """, {'srvid': srvid})
        repo_num = cur.fetchone()

        if (repo_num is None or repo_num[0] != server_num[0]):
            try:
                repo_cur.execute("""
                        UPDATE powa_servers
                        SET version = %(version)s
                        WHERE id = %(srvid)s
                        """, {'srvid': srvid, 'version': server_num[0]})
                self.__repo_conn.commit()
            except Exception as e:
                self.logger.warning("Could not save server version"
                                    + ": %s" % (e))
                self.__repo_conn.rollback()

        repo_cur.execute("""
            SELECT extname, version
            FROM powa_extensions
            WHERE srvid = %(srvid)s
            """ % {'srvid': srvid})
        exts = repo_cur.fetchall()

        for ext in exts:
            cur.execute("""
            --WITH raw AS (
            --   SELECT regexp_split_to_table(extversion, '\\.')::integer
            --    AS val
            --   FROM pg_extension
            --   WHERE extname = %(extname)s
            --)
            --SELECT string_agg(ltrim(to_char(val, '00')), '')::integer
            --FROM raw;
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
                            UPDATE powa_extensions
                            SET version = %(version)s
                            WHERE srvid = %(srvid)s
                            AND extname = %(extname)s
                            """, {'version': remote_ver,  'srvid': srvid,
                                  'extname': ext[0]})
                    self.__repo_conn.commit()
                except Exception as e:
                    self.logger.warning("Could not save version for extension "
                                        + "%s: %s" % (ext[0], e))
                    self.__repo_conn.rollback()

        self.__disconnect_repo()

    def __check_powa(self):
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
                    cur.execute("""UPDATE public.powa_snapshot_metas
                        SET errors = %s
                        WHERE srvid = %s
                    """, (error, srvid))
                else:
                    cur.execute("""UPDATE public.powa_snapshot_metas
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
        if ('dsn' not in self.__repository or 'dsn' not in self.__config):
            self.logger.error("Missing connection info")
            self.__stopping.set()
            return

        try:
            if (self.__repo_conn is None):
                self.logger.debug("Connecting on repository...")
                self.__repo_conn = psycopg2.connect(self.__repository['dsn'])
                self.logger.debug("Connected.")
                # make sure the GUC are present in case powa isn't in
                # shared_preload_librairies.  This is only required for powa
                # 4.0.x.
                self.__maybe_load_powa(self.__repo_conn)

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
                self.__remote_conn = psycopg2.connect(**self.__config['dsn'])
                self.logger.debug("Connected.")

                # make sure the GUC are present in case powa isn't in
                # shared_preload_librairies.  This is only required for powa
                # 4.0.x.
                if (self.__remote_conn is not None):
                    self.__maybe_load_powa(self.__remote_conn)

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
        if (self.__remote_conn is not None):
            self.logger.info("Disconnecting from remote server")
            self.__remote_conn.close()
            self.__remote_conn = None
        if (self.__repo_conn is not None):
            self.logger.info("Disconnecting from repository")
            self.__disconnect_repo()
        self.__connected.clear()

    def __disconnect_repo(self):
        if (self.__repo_conn is not None):
            self.__repo_conn.close()
            self.__repo_conn = None

    def __disconnect_all_and_exit(self):
        # this is the exit point
        self.__disconnect_all()
        self.logger.info("stopped")
        self.__stopping.clear()

    def __worker_main(self):
        self.last_time = None
        self.__check_powa()

        # if this worker has been restarted, restore the previous snapshot
        # time to try to keep up on the same frequency
        if (not self.is_stopping() and self.__repo_conn is not None):
            cur = None
            try:
                cur = self.__repo_conn.cursor()
                cur.execute("""SELECT EXTRACT(EPOCH FROM snapts)
                    FROM public.powa_snapshot_metas
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
                    self.last_time = row[0]
                    self.logger.debug("Retrieved last snapshot time:"
                                      + " %r" % self.last_time)
                cur.close()
                self.__repo_conn.commit()
            except Exception as e:
                self.logger.warning("Could not retrieve last snapshot"
                                    + " time: %s" % (e))
                if (cur is not None):
                    cur.close()
                self.__repo_conn.rollback()

        # if this worker was stopped longer than the configured frequency,
        # assign last snapshot time to a random time between now and now minus
        # duration.  This will help to spread the snapshots and avoid activity
        # spikes if the collector itself was stopped for a long time, or if a
        # lot of new servers were added
        if (not self.is_stopping()
            and self.last_time is not None
            and ((calendar.timegm(time.gmtime()) -
                 self.last_time) > self.__config["frequency"])):
            random.seed()
            r = random.randint(0, self.__config["frequency"] - 1)
            self.logger.debug("Spreading snapshot: setting last snapshot to"
                              + " %d seconds ago (frequency: %d)" %
                              (r, self.__config["frequency"]))
            self.last_time = calendar.timegm(time.gmtime()) - r

        while (not self.is_stopping()):
            cur_time = calendar.timegm(time.gmtime())
            if (self.__got_sighup.isSet()):
                self.__reload()

            if ((self.last_time is None) or
                    (cur_time - self.last_time) >= self.__config["frequency"]):
                try:
                    self.__take_snapshot()
                except psycopg2.Error as e:
                    self.logger.error("Error during snapshot: %s" % e)
                    if (self.__repo_conn is None
                            or self.__repo_conn.closed > 0):
                        self.__repo_conn = None
                    if (self.__remote_conn is None
                            or self.__remote_conn.closed > 0):
                        self.__remote_conn = None

                self.last_time = calendar.timegm(time.gmtime())
            time_to_sleep = self.__config["frequency"] - (cur_time -
                                                          self.last_time)

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

    def __take_snapshot(self):
        """
        Main part of the worker thread.  This function will call all the
        query_src functions enabled for the target server, and insert all the
        retrieved rows on the repository server, in unlogged tables, and
        finally call powa_take_snapshot() on the repository server to finish
        the distant snapshot.  All is done in one transaction, so that there
        won't be concurrency issues if a snapshot takes longer than the
        specified interval.
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

        # get the list of snapshot functions, and their associated query_src
        cur = self.__repo_conn.cursor(cursor_factory=DictCursor)
        cur.execute("SAVEPOINT snapshots")
        try:
            cur.execute(get_snapshot_functions(), (srvid,))
            snapfuncs = cur.fetchall()
            cur.execute("RELEASE snapshots")
        except psycopg2.Error as e:
            cur.execute("ROLLBACK TO snapshots")
            err = "Error while getting snapshot functions:\n%s" % (e)
            self.logger.error(err)
            self.logger.error("Exiting worker for server %s..." % srvid)
            self.__stopping.set()
            return
        cur.close()

        if (not snapfuncs):
            self.logger.info("No datasource configured for server %d" % srvid)
            self.logger.debug("Committing transaction")
            self.__repo_conn.commit()
            self.__disconnect_repo()
            return

        ins = self.__repo_conn.cursor()
        data_src = self.__remote_conn.cursor()

        errors = []
        for snapfunc in snapfuncs:
            if (self.is_stopping()):
                return

            module_name = snapfunc["module"]
            query_source = snapfunc["query_source"]
            cleanup_sql = snapfunc["query_cleanup"]
            function_name = snapfunc["function_name"]

            self.logger.debug("Working on module %s", module_name)

            # get the SQL needed to insert the query_src data on the remote
            # server into the transient unlogged table on the repository server
            if (query_source is None):
                self.logger.warning("Not query_source for %s" % function_name)
                continue

            # execute the query_src functions to get local data (srvid 0)
            self.logger.debug("Calling public.%s(0)..." % query_source)
            data_src_sql = get_src_query(query_source, srvid)

            # use savepoint, maybe the datasource is not setup on the remote
            # server
            data_src.execute("SAVEPOINT src")

            # XXX should we use os.pipe() or a temp file instead, to avoid too
            # much memory consumption?
            buf = StringIO()
            try:
                data_src.copy_expert("COPY (%s) TO stdout" % data_src_sql, buf)
            except psycopg2.Error as e:
                err = "Error while calling public.%s:\n%s" % (query_source, e)
                errors.append(err)
                data_src.execute("ROLLBACK TO src")

            # execute the cleanup query if provided
            if (cleanup_sql is not None):
                data_src.execute("SAVEPOINT src")
                try:
                    self.logger.debug("Calling %s..." % cleanup_sql)
                    data_src.execute(cleanup_sql)
                except psycopg2.Error as e:
                    err = "Error while calling %s:\n%s" % (cleanup_sql, e)
                    errors.append(err)
                    data_src.execute("ROLLBACK TO src")

            if (self.is_stopping()):
                return

            # insert the data to the transient unlogged table
            ins.execute("SAVEPOINT data")
            buf.seek(0, SEEK_SET)
            try:
                ins.copy_expert("COPY %s FROM stdin" %
                                get_tmp_name(query_source), buf)
            except psycopg2.Error as e:
                err = "Error while inserting data:\n%s" % e
                self.logger.warning(err)
                errors.append(err)
                self.logger.warning("Giving up for %s", function_name)
                ins.execute("ROLLBACK TO data")

            buf.close()

        data_src.close()

        if (self.is_stopping()):
            if (len(errors) > 0):
                self.__report_error(errors)
            return

        # call powa_take_snapshot() for the given server
        self.logger.debug("Calling powa_take_snapshot(%d)..." % (srvid))
        sql = ("SELECT public.powa_take_snapshot(%(srvid)d)" % {'srvid': srvid})
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
        return self.__stopping.isSet()

    def get_config(self):
        return self.__config

    def ask_to_stop(self):
        self.__stopping.set()
        self.logger.info("Asked to stop...")
        self.__stop_sleep.set()

    def run(self):
        if (not self.is_stopping()):
            self.logger.info("Starting worker")
            self.__worker_main()

    def ask_reload(self, new_config):
        self.logger.debug("Reload asked")
        self.__pending_config = new_config
        self.__got_sighup.set()
        self.__stop_sleep.set()

    def ask_update_dep_versions(self):
        self.logger.debug("Version dependencies reload asked")
        self.__update_dep_versions = True
        self.__got_sighup.set()
        self.__stop_sleep.set()

    def get_status(self):
        if (self.__repo_conn is None and self.__last_repo_conn_errored):
            return "no connection to repository server"
        if (self.__remote_conn is None):
            return "no connection to remote server"
        else:
            return "running"
