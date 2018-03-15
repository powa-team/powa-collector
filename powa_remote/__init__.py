"""
powa-remote main application.
"""

from powa_remote.options import parse_options
from powa_remote.powa_worker import PowaThread
import time
import logging
import signal

__VERSION__ = '0.0.1'
__VERSION_NUM__ =[int(part) for part in __VERSION__.split('.')]

from powa_remote.options import parse_options

class PowaRemote():
    def __init__(self, loglevel = None):
        self.workers = {}
        self.logger = logging.getLogger("powa-remote")
        self.stopping = False;

        if (loglevel is not None):
            loglevel = loglevel
        else:
            loglevel = logging.INFO

        extra = {'threadname': '-'}
        logging.basicConfig(format='%(asctime)s %(threadname)s: %(message)s ', level=loglevel)
        self.logger = logging.LoggerAdapter(self.logger, extra)
        signal.signal(signal.SIGHUP, self.sighandler)
        signal.signal(signal.SIGTERM, self.sighandler)

    def main(self):
        self.config = parse_options()
        self.logger.info("Starting powa-remote...")

        for s in self.config["servers"]:
            self.register_worker(s, self.config["repository"], self.config["servers"][s])

        try:
            while (not self.stopping):
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.debug("KeyboardInterrupt caught")
            self.logger.info("Stopping all workers and exiting...")
            self.stop_all_workers()

    def register_worker(self, name, repository, config):
            self.workers[name] = PowaThread(name, repository, config)
            #self.workers[s].daemon = True
            self.workers[name].start()

    def stop_all_workers(self):
        for k in self.workers:
            self.workers[k].ask_to_stop()

    def sighandler(self, signum, frame):
        if (signum == signal.SIGHUP):
            self.logger.debug("SIGHUP caught")
            self.reload_conf()
        elif (signum == signal.SIGTERM):
            self.logger.debug("SIGTERM caught")
            self.stop_all_workers()
            self.stopping = True
        else:
            self.logger.error("Unhandled signal %d" % signum);

    def reload_conf(self):
        self.logger.info('Reloading...')
        config_new = parse_options()

        # check for removed servers
        for k in self.workers:
            if (self.workers[k].isAlive()):
                continue

            if (self.workers[k].is_stopping()):
                self.logger.warn("Oops")

            if (k not in config_new["servers"]):
                self.logger.info("%s has been removed, stopping it..." % k)
                self.workers[k].ask_to_stop()

        # check for added servers
        for k in config_new["servers"]:
            if (k not in self.workers or not self.workers[k].isAlive()):
                self.logger.info("%s has been added, registering it..." % k)
                self.register_worker(k, config_new["repository"], config_new["servers"][k])

        # check for updated configuration
        for k in config_new["servers"]:
            cur = config_new["servers"][k]
            if (not conf_are_equal(cur, self.workers[k].get_config())):
                self.workers[k].ask_reload(cur)

        self.config = config_new

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
