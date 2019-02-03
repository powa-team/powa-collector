"""
powa-collector main application.
"""

from powa_collector.options import (parse_options, get_full_config,
                                    add_servers_config)
from powa_collector.powa_worker import PowaThread
import psycopg2
import time
import logging
import signal

__VERSION__ = '0.0.1'
__VERSION_NUM__ = [int(part) for part in __VERSION__.split('.')]


class PowaCollector():
    def __init__(self, loglevel=logging.INFO):
        self.workers = {}
        self.logger = logging.getLogger("powa-collector")
        self.stopping = False

        extra = {'threadname': '-'}
        logging.basicConfig(
                format='%(asctime)s %(threadname)s %(levelname)-6s: %(message)s ',
                level=loglevel)
        self.logger = logging.LoggerAdapter(self.logger, extra)
        signal.signal(signal.SIGHUP, self.sighandler)
        signal.signal(signal.SIGTERM, self.sighandler)

    def connect(self, options):
        try:
            self.logger.debug("Connecting on repository...")
            self.__repo_conn = psycopg2.connect(options["repository"]['dsn'])
            self.logger.debug("Connected.")
            cur = self.__repo_conn.cursor()
            cur.execute("SET application_name = %s",
                        ('PoWA collector - main thread'
                         + ' (' + __VERSION__ + ')', ))
            cur.execute("LISTEN powa_collector")
            cur.close()
        except psycopg2.Error as e:
            self.__repo_conn = None
            self.logger.error("Error connecting:\n%s", e)
            return False

        return True

    def main(self):
        raw_options = parse_options()
        self.logger.info("Starting powa-collector...")

        if (not self.connect(raw_options)):
            exit(1)

        self.config = add_servers_config(self.__repo_conn, raw_options)

        for k, conf in self.config["servers"].items():
            self.register_worker(k, self.config["repository"], conf)

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

                time.sleep(10)
        except KeyboardInterrupt:
            self.logger.debug("KeyboardInterrupt caught")
            self.logger.info("Stopping all workers and exiting...")
            self.stop_all_workers()

    def register_worker(self, name, repository, config):
            self.workers[name] = PowaThread(name, repository, config)
            # self.workers[s].daemon = True
            self.workers[name].start()

    def stop_all_workers(self):
        for k, worker in self.workers.items():
            worker.ask_to_stop()

    def sighandler(self, signum, frame):
        if (signum == signal.SIGHUP):
            self.logger.debug("SIGHUP caught")
            self.reload_conf()
        elif (signum == signal.SIGTERM):
            self.logger.debug("SIGTERM caught")
            self.stop_all_workers()
            self.stopping = True
        else:
            self.logger.error("Unhandled signal %d" % signum)

    def list_workers(self):
        self.logger.info('List of workers:')
        for k, worker in self.workers.items():
            # self.logger.info(" %s%s" % (k, "" if (worker.isAlive()) else
            #                             " (stopped)"))
            self.logger.info("%r%s" % (worker, "" if (worker.isAlive()) else
                                       " (stopped)"))

    def reload_conf(self):
        self.list_workers()

        self.logger.info('Reloading...')
        config_new = get_full_config(self.__repo_conn)

        # check for removed servers
        for k, worker in self.workers.items():
            if (worker.isAlive()):
                continue

            if (worker.is_stopping()):
                self.logger.warning("Oops")

            if (k not in config_new["servers"]):
                self.logger.info("%s has been removed, stopping it..." % k)
                worker.ask_to_stop()

        # check for added servers
        for k in config_new["servers"]:
            if (k not in self.workers or not self.workers[k].isAlive()):
                self.logger.info("%s has been added, registering it..." % k)
                self.register_worker(k, config_new["repository"],
                                     config_new["servers"][k])

        # check for updated configuration
        for k in config_new["servers"]:
            cur = config_new["servers"][k]
            if (not conf_are_equal(cur, self.workers[k].get_config())):
                self.workers[k].ask_reload(cur)

        self.config = config_new
        self.logger.info('Reload done')


def conf_are_equal(conf1, conf2):
    for k in conf1.keys():
        if (k not in conf2):
            return False
        if (conf1[k] != conf2[k]):
            return False

    for k in conf2.keys():
        if (k not in conf1):
            return False
        if (conf1[k] != conf2[k]):
            return False

    return True
