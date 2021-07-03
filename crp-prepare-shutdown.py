#!/usr/bin/env python

import argparse
import pymysql.cursors
import crptoolkit

def mysql_options():
    parser = argparse.ArgumentParser()
    parser.add_argument("-u", "--user", type=str, dest="user", help="MySQL user.")
    parser.add_argument("-p", "--password", type=str, dest="password", metavar="PASS", help="MySQL password.")
    parser.add_argument("--ask-pass", dest="ask_pass", action="store_true", help="Ask for password.")
    parser.add_argument("-H", "--host", type=str, dest="host", help="MySQL host. Default: localhost")
    parser.add_argument("-P", "--port", type=int, dest="port", help="MySQL port. Default: 3306")
    parser.add_argument("-S", "--socket", type=str, dest="socket", metavar="SOCK",
                        help="MySQL socket. Default: /var/lib/mysql/mysql.sock")
    parser.add_argument("--defaults-file", dest="defaults_file", metavar="FILE", help="Use MySQL configuration file.")
    parser.add_argument("-t", "--no-transaction-check", action="store_true", dest="no_transaction_check",
                        help="Do not check for transactions running > 60 seconds.")
    parser.add_argument("-v", "--verbose", dest="verbose", action="store_true", help="Print additional information.")
    return parser.parse_args()


class MySQLPrepareShutdown:
    def __init__(self):
    mysql = crptoolkit.MySQL()
    logger = crptoolkit.Logger()
    logger.info("[ START ] Preparing MySQL for shutdown.")

    # Check if the host is a replica.
    is_replica = int(mysql_check_is_replica())
    if is_replica:
        slave_parallel_workers = int(mysql_get_global_variable("slave_parallel_workers"))
        if slave_parallel_workers > 0:
            error("This is a multi-threaded replica.")
        else:
            mysql_stop_replica_single_thread()
    # Check for long running transactions.
    if options.no_transaction_check is None:
        mysql_check_long_transactions(is_replica)
    else:
        warn("--no-transaction-check was used. Not checking for long running transactions.")
    # Get dirty pages details.
    dirty_pages_pct_original = float(mysql_get_global_variable("innodb_max_dirty_pages_pct"))
    verbose("innodb_max_dirty_pages_pct was %s." % dirty_pages_pct_original)
    dirty_pages_start = int(mysql_get_status_variable("Innodb_buffer_pool_pages_dirty"))
    # Set dirty pages to 0 and check that they're low enough.
    mysql_set_dirty_pages_pct(0)
    try:
        mysql_check_dirty_pages(dirty_pages_start)
    except KeyboardInterrupt:
        warn("CTL+C. Reverting changes.")
        mysql_set_dirty_pages_pct(dirty_pages_pct_original)
        if is_replica:
            mysql_start_replica_single_thread()
        error("Terminated.")
    # Set fast shutdown to 0.
    mysql_set_fast_shutdown()
    # Set buffer pool dump configurations.
    mysql_set_buffer_pool_dump()
    info("[ COMPLETED ] MySQL is prepared for shutdown!")

if __name__ == "__main__":
    try:
        conn = None
        # (options, args) = mysql_options()
        options = mysql_options()
        conn = mysql_connect()
        mysql_prepare_shutdown()
    except pymysql.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))
    finally:
        if conn:
            conn.close()