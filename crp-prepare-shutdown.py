#!/usr/bin/env python

import argparse
from time import sleep, time
from crptoolkit.args import ArgParser
from crptoolkit.logger import Logger
from crptoolkit.mysql import MySQL

def args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-u", "--user", type=str, dest="user", help="MySQL user")
    parser.add_argument("-p", "--password", type=str, dest="password", metavar="PASS", help="MySQL password")
    parser.add_argument("--ask-pass", dest="ask_pass", action="store_true", help="Ask for password")
    parser.add_argument("-H", "--host", type=str, dest="host", help="MySQL host (default: 127.0.0.1)")
    parser.add_argument("-P", "--port", type=int, dest="port", help="MySQL port (default: 3306)")
    parser.add_argument("-S", "--socket", type=str, dest="socket", metavar="SOCK",
                        help="MySQL socket")
    parser.add_argument("--defaults-file", dest="defaults_file", metavar="FILE", help="Use MySQL configuration file")
    parser.add_argument("-t", "--no-transaction-check", action="store_true", dest="no_transaction_check",
                        help="Do not check for transactions > 60 seconds")
    parser.add_argument("-v", "--verbose", dest="verbose", action="store_true", help="Print additional tool information")
    return parser.parse_args()

class MySQLPrepareShutdown:

    def __init__(self, args):
        self.args = ArgParser(args)
        self.defaults_file, self.host, self.port, self.user, self.password, self.socket = self.args.connect()
        self.mysql = MySQL(self.defaults_file, self.host, self.port, self.user, self.password, self.socket)
        self.connect = self.mysql.connect()
        self.log = Logger(args.verbose)

    def run(self):
        self.log.info("Preparing MySQL for shutdown.", "START")

        self.log.verbose("Checking if this is a replica.")
        if (is_replica := self.mysql.is_replica()):
            self.log.info("This is a replica. Stopping replication.")
            if int(self.mysql.get_variable("slave_parallel_workers")) > 0:
                self.log.error("This is a multi-threaded replica.")
            else:
                self.log.verbose("Stopping IO thread.")
                self.mysql.stop_replication_io_thread()
                self.log.verbose("Giving SQL thread 10 seconds to catch up.")
                sleep(10)
                self.log.verbose("Stopping SQL thread.")
                self.mysql.stop_replication_sql_thread()
            self.log.info("Replication stopped.")

        if args.no_transaction_check:
            self.log.warn("--no-transaction-check was used. Not checking for long running transactions.")
        else:
            self.log.verbose("Checking for transactions running > 60s.")
            if self.mysql.get_transactions(60):
                if is_replica:
                    self.log.warn("Restarting replication.")
                    self.mysql.start_replication()
                self.log.error("Transaction(s) found running > 60 seconds. COMMIT, ROLLBACK, or kill them. Otherwise, use the less safe `--no-transaction-check`.")
            self.log.info("No transactions found running > 60 seconds.")

        dirty_pages_pct_original = float(self.mysql.get_variable("innodb_max_dirty_pages_pct"))
        dirty_pages_start = int(self.mysql.get_status_variable("Innodb_buffer_pool_pages_dirty"))
        self.log.info("Setting innodb_max_dirty_pages_pct -> 0.0.")
        self.mysql.set_variable("innodb_max_dirty_pages_pct", 0.0)
        timeout = time() + 60
        try:
            while True:
                dirty_pages_current = int(self.mysql.get_status_variable("Innodb_buffer_pool_pages_dirty"))
                if dirty_pages_current == 0:
                    self.log.verbose("Dirty pages = 0. Continuing to prepare for shutdown.")
                    break
                elif dirty_pages_current < (int(dirty_pages_start) * .10):
                    self.log.verbose("Dirty pages < 10%. Continuing to prepare for shutdown.")
                    break
                elif int(dirty_pages_current) < 500:
                    self.log.verbose("Dirty pages < 500. Continuing to prepare for shutdown.")
                    break
                elif time() > timeout:
                    self.log.warn("Its been 1 minute. Dirty pages may still be high. Continuing to prepare for shutdown.")
                    break
                else:
                    self.log.info(f"Dirty pages = {dirty_pages_current}, waiting up to 1 minute for it to lower.")
                    sleep(5)
        except KeyboardInterrupt:
            self.mysql.set_variable("innodb_max_dirty_pages_pct", dirty_pages_pct_original)
            if is_replica:
                self.log.warn("Restarting replication.")
                self.mysql.start_replication()
                self.log.error("Received CTL+C. Reverted innodb_max_dirty_pages_pct and restarted replication before exiting.")
            self.log.error("Received CTL+C. Reverted innodb_max_dirty_pages_pct before exiting.")

        self.log.info("Setting innodb_fast_shutdown -> 0.")
        self.mysql.set_variable("innodb_fast_shutdown", 0)

        self.log.info("Setting innodb_buffer_pool_dump_at_shutdown -> 1.")
        self.mysql.set_variable("innodb_buffer_pool_dump_at_shutdown", 1)

        self.log.info("Setting innodb_buffer_pool_dump_pct -> 75.")
        self.mysql.set_variable("innodb_buffer_pool_dump_pct", 75)

        if self.mysql.get_variable("innodb_buffer_pool_load_at_startup") == 0:
            self.log.warn("innodb_buffer_pool_load_at_startup = 0. You may want to enable this in the my.cnf: innodb_buffer_pool_load_at_startup = ON")

        self.log.info("MySQL is prepared for shutdown!", "COMPLETED")

if __name__ == "__main__":
    args = args()
    MySQLPrepareShutdown(args).run()
