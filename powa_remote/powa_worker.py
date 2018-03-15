import threading
import time
import calendar
import psycopg2
import logging

class PowaThread (threading.Thread):
    def __init__(self, name, repository, config):
        threading.Thread.__init__(self)
        self.__stopping = threading.Event()
        self.__got_sighup = threading.Event()
        self.__connected = threading.Event()
        self.name = name
        self.__repository = repository
        self.__config = config
        self.__pending_config = None
        self.__remote_conn = None
        self.__repo_conn = None;
        self.logger = logging.getLogger("powa-remote")
        self.last_time = None

        extra = {'threadname': self.name}
        self.logger = logging.LoggerAdapter(self.logger, extra)

    def __check_powa(self):
        if (self.__remote_conn is not None):
            cur = self.__remote_conn.cursor()
            cur.execute("SELECT COUNT(*) FROM pg_extension WHERE extname = 'powa'")
            res = cur.fetchone()
            cur.close()

            if (res[0] != 1):
                self.logger.error("PoWA extension not found")
                self.__disconnect()
                self.__stopping.set()

    def __reload(self):
        self.logger.info("Reloading configuration")
        self.__config = self.__pending_config
        self.__pending_config = None
        self.__disconnect()
        self.__connect()
        self.__got_sighup.clear()

    def __connect(self):
        if ('dsn' not in self.__repository or 'dsn' not in self.__config):
            self.logger.error("Missing connection info")
            self.__stopping.set()
            return

        try:
            self.logger.debug("Connecting on repository...")
            self.__repo_conn = psycopg2.connect(self.__repository['dsn'])
            self.logger.debug("Connected.")

            self.logger.debug("Connecting on remote database...")
            self.__remote_conn = psycopg2.connect(self.__config['dsn'])
            self.logger.debug("Connected.")
            self.__connected.set()
        except:
            self.logger.error("Error connecting")
            self.__disconnect()
            self.__stopping.set()

    def __disconnect(self):
        if (self.__remote_conn is not None):
            self.logger.info("Disconnecting from remote server")
            self.__remote_conn.close()
            self.__remote_conn = None
        if (self.__repo_conn is not None):
            self.logger.info("Disconnecting from repository")
            self.__repo_conn.close()
            self.__repo_conn = None
        self.__connected.clear()

    def __disconnect_and_exit(self):
        self.__disconnect()
        self.logger.info("stopped")
        self.__stopping.clear()

    def __worker_main(self):
        self.__connect()
        self.last_time = calendar.timegm(time.gmtime())
        self.__check_powa()
        while (not self.__stopping.isSet()):
            cur_time = calendar.timegm(time.gmtime())
            if (self.__got_sighup.isSet()):
                self.__reload()

            if ((cur_time - self.last_time) >= self.__config["frequency"]):
                self.__take_snapshot()
                self.last_time = calendar.timegm(time.gmtime())
            time.sleep(0.1)

        self.__disconnect_and_exit()

    def __take_snapshot(self):
        cur = self.__remote_conn.cursor()
        cur.execute("""SELECT function_name FROM powa_functions
                    WHERE operation = 'snapshot' AND enabled""")
        rows = cur.fetchall()
        cur.close()

        for f in rows:
            self.logger.debug("Should call %s()" % f[0])

    def is_stopping(self):
        return self.__stopping.isSet()

    def get_config(self):
        return self.__config

    def ask_to_stop(self):
        self.__stopping.set()
        self.logger.info("Asked to stop...")

    def run(self):
        if (not self.__stopping.isSet()):
            self.logger.info("Starting worker")
            self.__worker_main()

    def ask_reload(self, new_config):
        self.logger.debug("Reload asked")
        self.__pending_config = new_config
        self.__got_sighup.set()

