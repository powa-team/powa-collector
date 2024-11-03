"""
PowaCollector: powa-collector main application.

It takes a simple configuration file in json format, where repository.dsn
should point to an URI to connect to the repository server.  The list of remote
servers and their configuration will be retrieved from this repository server.

It maintains a persistent dedicated connection to the repository server, for
monitoring and communication purpose.  It also starts one thread per remote
server.  These threads are kept in the "workers" dict attribute, with the key
being the textual identifier (host:port).  See powa_worker.py for more details
about those threads.

The main thread will intercept the following signals:

    - SIGHUP: reload configuration and log and changes done
    - SIGTERM: cleanly terminate all threads and exit

A minimal communication protocol is implemented, using the LISTEN/NOTIFY
facility provided by postgres.  The dedicated main thread repository connection
listens on the "powa_collector" channel.  A client, such as powa-web, can send
requests on this channel and the main thread will act and respond accordingly.

The requests are of the following form:

    COMMAND RESPONSE_CHANNEL OPTIONAL_ARGUMENTS

See the README.md file for the full protocol documentation.
"""

from powa_collector.options import (parse_options, get_full_config,
                                    add_servers_config)
from powa_collector.powa_worker import PowaThread
from powa_collector.customconn import get_connection
from powa_collector.notify import (notify_parse_force_snapshot,
                                   notify_parse_refresh_db_cat,
                                   notify_allowed)
from powa_collector.utils import conf_are_equal
import psycopg2
import select
import logging
import json
import signal
import time

__VERSION__ = '1.3.0'
__VERSION_NUM__ = [int(part) for part in __VERSION__.split('.')]


def getVersion():
    """Return powa_collector's version as a string"""
    return __VERSION__


class PowaCollector():
    """Main powa collector's class. This manages all collection tasks
    Declare all attributes here, we don't want dynamic attributes
    """
    def __init__(self):
        """Instance creator. Sets logging, signal handlers, and basic structure"""
        self.workers = {}
        self.logger = logging.getLogger("powa-collector")
        self.stopping = False

        raw_options = parse_options()
        loglevel = logging.INFO
        if (raw_options["debug"]):
            loglevel = logging.DEBUG

        extra = {'threadname': '-'}
        logging.basicConfig(
                format='%(asctime)s %(threadname)s %(levelname)-6s: %(message)s ',
                level=loglevel)
        self.logger = logging.LoggerAdapter(self.logger, extra)
        signal.signal(signal.SIGHUP, self.sighandler)
        signal.signal(signal.SIGTERM, self.sighandler)


    def __get_worker_by_srvid(self, srvid):
        """Get the worker thread for the given srvid, if any"""
        for k, worker in self.workers.items():
            worker_srvid = self.config["servers"][k]["srvid"]

            if (srvid != worker_srvid):
                continue

            return worker
        return None

    def connect(self, options):
        """Connect to the repository
        Used for communication with powa-web and users of the communication repository
        Persistent
        Threads will use distinct connections
        """
        try:
            self.logger.debug("Connecting on repository...")
            self.__repo_conn = get_connection(self.logger,
                                              options["debug"],
                                              options["repository"]['dsn'])
            self.__repo_conn.autocommit = True
            self.logger.debug("Connected.")
            cur = self.__repo_conn.cursor()

            # Setup a 2s lock_timeout if there's no inherited lock_timeout
            cur.execute("""SELECT
                pg_catalog.set_config(name, '2000', false)
                FROM pg_catalog.pg_settings
                WHERE name = 'lock_timeout'
                AND setting = '0'""")
            cur.execute("SET application_name = %s",
                        ('PoWA collector - main thread'
                         + ' (' + __VERSION__ + ')', ))

            # Listen on our dedicated powa_collector notify channel
            cur.execute("LISTEN powa_collector")

            # Check if powa-archivist is installed on the repository server
            cur.execute("""SELECT
                    regexp_split_to_array(extversion, '\\.'),
                    extversion
                FROM pg_catalog.pg_extension
                WHERE extname = 'powa'""")
            ver = cur.fetchone()
            cur.close()

            if ver is None:
                self.__repo_conn.close()
                self.__repo_conn = None
                self.logger.error("PoWA extension not found on repository "
                                  "server")
                return False
            elif (int(ver[0][0]) < 4):
                self.__repo_conn.close()
                self.__repo_conn = None
                self.logger.error("Incompatible PoWA version, found %s,"
                                  " requires at least 4.0.0" % ver[1])
                return False

        except psycopg2.Error as e:
            self.__repo_conn = None
            self.logger.error("Error connecting:\n%s", e)
            return False

        return True

    def process_notifications(self):
        """Process PostgreSQL NOTIFY messages.
        These come mainly from the UI, to ask us to reload our configuration,
        or to display the workers status.
        """
        if (not self.__repo_conn):
            return

        self.__repo_conn.poll()
        cur = self.__repo_conn.cursor()

        while (self.__repo_conn.notifies):
            notify = self.__repo_conn.notifies.pop(0)
            pid = notify.pid
            payload = notify.payload.split(' ')
            status = ''
            cmd = payload.pop(0)
            channel = "-"
            status = "OK"
            data = None

            # the channel is mandatory, but if the message can be processed
            # without answering, we'll try to
            if (len(payload) > 0):
                channel = payload.pop(0)

            self.logger.debug("Received async command: %s %s %r" %
                              (cmd, channel, payload))

            if (not notify_allowed(pid, self.__repo_conn)):
                status = 'ERROR'
                data = 'Permission denied'
            else:
                try:
                    (status, data) = self.__process_one_notification(cmd,
                                                                     channel,
                                                                     payload)
                except Exception as e:
                    status = 'ERROR'
                    data = str(e)

            # if there was a response channel, reply back
            if (channel != '-'):
                # We need an extra {} around the data as the custom connection
                # will call format() on the overall string, which would fail
                # with a json dump as-is.  We therefore also need to make sure
                # that any non-empty data looks like a json.

                payload = ("%(cmd)s %(status)s %(data)s" %
                           {'cmd': cmd, 'status': status, 'data': data})

                # with default configuration, postgres only accept up to 8k
                # bytes payload.  If the payload is longer, just warn the
                # caller that it didn't fit.
                # XXX we could implement multi-part answer, but if we ever
                # reach that point, we should consider moving to a table
                if (len(payload.encode('utf-8')) >= 8000):
                    payload = ("%(cmd)s %(status)s %(data)s" %
                               {'cmd': cmd,
                                'status': "KO",
                                'data': "{ANSWER TOO LONG}"})

                cur.execute('NOTIFY "' + channel + '", %(payload)s',
                            {'payload': payload})

        cur.close()

    def __process_one_notification(self, cmd, channel, notif):
        """
        Process a single notification, called by process_notifications.
        """
        status = "OK"
        data = ''

        if (cmd == "RELOAD"):
            self.reload_conf()
            data = 'OK'
        elif (cmd == "WORKERS_STATUS"):
            # ignore the message if no channel was received
            if (channel != '-'):
                # did the caller request a single server only?  We ignore
                # anything but the first parameter passed
                if (len(notif) > 0 and notif[0].isdigit()):
                    w_id = int(notif[0])
                    data = json.dumps(self.list_workers(w_id, False))
                else:
                    data = json.dumps(self.list_workers(None, False))
        elif (cmd == "FORCE_SNAPSHOT"):
            r_srvid = notify_parse_force_snapshot(notif)
            worker = self.__get_worker_by_srvid(r_srvid)

            if (worker is None):
                raise Exception("Server id %d not found" % r_srvid)

            request_time = time.time()
            (status, data) = worker.request_forced_snapshot()

            # If a snapshot could be scheduled, wait just a bit and see if we
            # can report that the snapshot has already begun or will be started
            # shortly.  It's possible, although unlikely, that the snapshot was
            # started and finished during that time.  We don't try to properly
            # detect that case, as it should only happen in toy setups.  We
            # might want to change that later if we want to provide a way to
            # inform callers of the completion of the snapshot they requested.
            if (status == 'OK'):
                time.sleep(0.1)
                if worker.is_snapshot_in_progress():
                    data = 'Snapshot in progress'
                else:
                    data = 'Snapshot wil begin shortly'
        elif (cmd == "REFRESH_DB_CAT"):
            (r_srvid, r_dbnames) = notify_parse_refresh_db_cat(notif)
            worker = self.__get_worker_by_srvid(r_srvid)

            if (worker is None):
                raise Exception("Server id %d not found" % r_srvid)

            (status, data) = worker.register_cat_refresh(r_dbnames)

        # everything else is unhandled
        else:
            status = 'UNKNOWN'
            data = 'Message "%s" invalid, command ignored' % cmd

        return (status, data)

    def main(self):
        """Start the active loop.
        Connect or reconnect to the repository and starts threads to manage the
        monitored servers
        """
        raw_options = parse_options()
        self.logger.info("Starting powa-collector...")

        if (not self.connect(raw_options)):
            exit(1)

        try:
            self.config = add_servers_config(self.__repo_conn, raw_options)
        except psycopg2.Error as e:
            self.__repo_conn.close()
            self.__repo_conn = None
            self.logger.error("Error retrieving the list of remote servers:"
                              "\n%s",
                              e)
            exit(1)

        for k, conf in self.config["servers"].items():
            self.register_worker(k, self.config["repository"], conf,
                                 raw_options['debug'])

        self.list_workers()

        try:
            while (not self.stopping):
                if (self.__repo_conn is not None):
                    try:
                        cur = self.__repo_conn.cursor()
                        cur.execute("SELECT 1")
                        cur.close()
                    except Exception:
                        self.__repo_conn = None
                        self.logger.warning("Connection was dropped!")

                if (not self.__repo_conn):
                    self.connect(raw_options)

                select.select([self.__repo_conn], [], [], 10)

                self.process_notifications()
        except KeyboardInterrupt:
            self.logger.debug("KeyboardInterrupt caught")
            self.logger.info("Stopping all workers and exiting...")
            self.stop_all_workers()

    def register_worker(self, name, repository, config, debug):
        """Add a worker thread to a server"""
        self.workers[name] = PowaThread(name, repository, config, debug)
        self.workers[name].start()

    def stop_all_workers(self):
        """Ask all worker threads to stop
        This is asynchronous, no guarantee
        """
        for k, worker in self.workers.items():
            worker.ask_to_stop()

    def sighandler(self, signum, frame):
        """Manage signal handlers: reload conf on SIGHUB, shutdown on SIGTERM"""
        if (signum == signal.SIGHUP):
            self.logger.debug("SIGHUP caught")
            self.reload_conf()
        elif (signum == signal.SIGTERM):
            self.logger.debug("SIGTERM caught")
            self.stop_all_workers()
            self.stopping = True
        else:
            self.logger.error("Unhandled signal %d" % signum)

    def list_workers(self, wanted_srvid=None, tostdout=True):
        """List all workers and their status"""
        res = {}

        if (tostdout):
            self.logger.info('List of workers:')

        if (tostdout and len(self.workers.items()) == 0):
            self.logger.info('No worker')

        for k, worker in self.workers.items():
            # self.logger.info(" %s%s" % (k, "" if (worker.is_alive()) else
            #                             " (stopped)"))
            worker_srvid = self.config["servers"][k]["srvid"]

            # ignore this entry if caller want information for only one server
            if (wanted_srvid is not None and wanted_srvid != worker_srvid):
                continue

            status = "Unknown"
            if (worker.is_stopping()):
                status = "stopping"
            elif (worker.is_alive()):
                status = worker.get_status()
            else:
                status = "stopped"

            if (tostdout):
                self.logger.info("%r (%s)" % (worker, status))
            else:
                res[worker_srvid] = status

        return res

    def reload_conf(self):
        """Reload configuration:
        - reparse the configuration
        - stop and start workers if necessary
        - for those whose configuration has changed, ask them to reload
        - update dep versions: recompute powa version's and its dependencies
        """
        self.list_workers()

        self.logger.info('Reloading...')
        config_new = get_full_config(self.__repo_conn)

        # check for removed servers
        for k, worker in self.workers.items():
            if (worker.is_alive()):
                continue

            if (worker.is_stopping()):
                self.logger.warning("The worker %s is stoping" % k)

            if (k not in config_new["servers"]):
                self.logger.info("%s has been removed, stopping it..." % k)
                worker.ask_to_stop()

        # check for added servers
        for k in config_new["servers"]:
            if (k not in self.workers or not self.workers[k].is_alive()):
                self.logger.info("%s has been added, registering it..." % k)
                self.register_worker(k, config_new["repository"],
                                     config_new["servers"][k],
                                     config_new['debug'])

        # check for updated configuration
        for k in config_new["servers"]:
            cur = config_new["servers"][k]
            if (not conf_are_equal(cur, self.workers[k].get_config())):
                self.workers[k].ask_reload(cur)
            # also try to reconnect if the worker experienced connection issue
            elif(self.workers[k].get_status() != "running"):
                self.workers[k].ask_reload(cur)

        # update stored versions
        for k in config_new["servers"]:
            self.workers[k].ask_update_dep_versions()

        self.config = config_new
        self.logger.info('Reload done')
