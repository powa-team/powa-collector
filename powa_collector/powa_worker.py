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

    def __check_powa(self):
        if (self.__remote_conn is None):
            self.__connect()

        if (self.is_stopping()):
            return

        if (self.__remote_conn is not None):
            cur = self.__remote_conn.cursor()
            cur.execute("""SELECT
                (split_part(extversion, '.', 1))::int,
                extversion
                FROM pg_catalog.pg_extension
                WHERE extname = 'powa'""")
            res = cur.fetchone()
            cur.close()

            if (not res):
                self.logger.error("PoWA extension not found")
                self.__disconnect_all()
                self.__stopping.set()
                return
            elif (res[0] < 4):
                self.logger.error("Incompatible PoWA version, found %s,"
                                  " requires at least 4.0.0" % res[1])
                self.__disconnect_all()
                self.__stopping.set()
                return

            try:
                # make sure the GUC are present in case powa isn't in
                # shared_preload_librairies
                cur = self.__remote_conn.cursor()
                cur.execute("LOAD 'powa'")
                cur.close()
                self.__remote_conn.commit()
            except psycopg2.Error as e:
                self.logger.error("Could not load extension powa:\n%s" % e)
                self.__disconnect_all()
                self.__stopping.set()

    def __reload(self):
        self.logger.info("Reloading configuration")
        self.__config = self.__pending_config
        self.__pending_config = None
        self.__disconnect_all()
        self.__connect()
        self.__got_sighup.clear()

    def __report_error(self, msg, replace=True):
        if (self.__repo_conn is not None):
            if (type(msg).__name__ == 'list'):
                error = msg
            else:
                error = [msg]
            cur = self.__repo_conn.cursor()
            if (replace):
                cur.execute("""UPDATE public.powa_snapshot_metas
                    SET errors = %s
                    WHERE srvid = %s
                """, (error, self.__config["srvid"]))
            else:
                cur.execute("""UPDATE public.powa_snapshot_metas
                    SET errors = pg_catalog.array_cat(errors, %s)
                    WHERE srvid = %s
                """, (error, self.__config["srvid"]))
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
                cur = self.__repo_conn.cursor()
                # make sure the GUC are present in case powa isn't in
                # shared_preload_librairies
                cur.execute("LOAD 'powa'")
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
                cur = self.__remote_conn.cursor()
                # make sure the GUC are present in case powa isn't in
                # shared_preload_librairies
                cur.execute("LOAD 'powa'")
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
                self.last_time = cur.fetchone()[0]
                cur.close()
                self.__repo_conn.commit()
                self.logger.debug("Retrieved last snapshot time:"
                                  + " %r" % self.last_time)
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
            # asked us to perform an action
            if (time_to_sleep > 0):
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
        cur = self.__repo_conn.cursor()
        cur.execute(get_snapshot_functions(), (srvid,))
        snapfuncs = cur.fetchall()
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

            # get the SQL needed to insert the query_src data on the remote
            # server into the transient unlogged table on the repository server
            if (snapfunc[0] is None):
                self.logger.warning("Not query_source for %s" % snapfunc[1])
                continue

            # execute the query_src functions to get local data (srvid 0)
            self.logger.debug("Calling public.%s(0)..." % snapfunc[0])
            data_src_sql = get_src_query(snapfunc[0], srvid)

            # use savepoint, maybe the datasource is not setup on the remote
            # server
            data_src.execute("SAVEPOINT src")

            # XXX should we use os.pipe() or a temp file instead, to avoid too
            # much memory consumption?
            buf = StringIO()
            try:
                data_src.copy_expert("COPY (%s) TO stdout" % data_src_sql, buf)
            except psycopg2.Error as e:
                err = "Error while calling public.%s:\n%s" % (snapfunc[0], e)
                errors.append(err)
                data_src.execute("ROLLBACK TO src")

            if (self.is_stopping()):
                return

            # insert the data to the transient unlogged table
            ins.execute("SAVEPOINT data")
            buf.seek(0, SEEK_SET)
            try:
                ins.copy_expert("COPY %s FROM stdin" %
                                get_tmp_name(snapfunc[0]), buf)
            except psycopg2.Error as e:
                err = "Error while inserting data:\n%s" % e
                self.logger.warning(err)
                errors.append(err)
                self.logger.warning("Giving up for %s", snapfunc[1])
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
        ins.execute(sql)
        val = ins.fetchone()[0]
        if (val != 0):
            self.logger.warning("Number of errors during snapshot: %d", val)
            self.logger.warning("  Check the logs on the repository server")

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

    def get_status(self):
        if (self.__repo_conn is None and self.__last_repo_conn_errored):
            return "no connection to repository server"
        if (self.__remote_conn is None):
            return "no connection to remote server"
        else:
            return "running"
