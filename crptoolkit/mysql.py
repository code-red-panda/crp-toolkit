import configparser
import getpass
import pymysql.cursors
import os.path
from time import sleep, time
from prettytable import PrettyTable

class MySQL:

    def connect(self):
        if options.defaults_file is not None:
            connection = pymysql.connect(read_default_file=options.defaults_file)
        else:
            try:
                dot_my_cnf = os.path.expanduser("~/.my.cnf")
                parser = configparser.ConfigParser()
                parser.read(dot_my_cnf)
                has_dot_my_cnf = 1
            except:
                has_dot_my_cnf = None
            conn_host = "localhost"
            if options.host:
                conn_host = options.host
            elif has_dot_my_cnf:
                try:
                    conn_host = parser.get('client', 'host')
                except:
                    pass
            conn_user = None
            if options.user:
                conn_user = options.user
            elif has_dot_my_cnf:
                try:
                    conn_user = parser.get('client', 'user')
                except:
                    pass
            conn_password = None
            if options.ask_pass:
                conn_password = getpass.getpass()
            elif options.password:
                conn_password = options.password
            elif has_dot_my_cnf:
                try:
                    conn_password = parser.get('client', 'password')
                except:
                    pass
            conn_socket = "/var/lib/mysql/mysql.sock"
            if options.socket:
                conn_socket = options.socket
            elif has_dot_my_cnf:
                try:
                    conn_socket = parser.get('client', 'socket')
                except:
                    pass
            connection = pymysql.connect(
                host=conn_host,
                user=conn_user,
                password=conn_password,
                unix_socket=conn_socket)
        return connection

    def run_query(self, sql):
        with conn.cursor() as cursor:
            result = cursor.execute(sql)
        cursor.close()
        return result

    def get_variable(self, variable_name):
        sql = f"SHOW GLOBAL VARIABLES WHERE VARIABLE_NAME={variable_name}"
        result = self.run_query(self, sql)
        return result[1]

    def get_status_variable(self, variable_name):
        sql = f"SHOW GLOBAL STATUS WHERE VARIABLE_NAME={variable_name}"
        result = self.run_query(self, sql)
        return result[1]

    def is_replica():
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            sql = "SHOW SLAVE STATUS"
            cursor.execute(sql)
            result = cursor.fetchone()
        cursor.close()
        if result is None:
            verbose("This is not a replica. Skipping replication tasks.")
            value = 0
        else:
            verbose("This is a replica.")
            value = 1
        return value


    def mysql_stop_replica_single_thread():
        info("Stopping replication.")
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            sql = "SHOW SLAVE STATUS"
            cursor.execute(sql)
            result = cursor.fetchone()
        cursor.close()
        slave_io_running = result["Slave_IO_Running"]
        slave_sql_running = result["Slave_SQL_Running"]
        if slave_io_running == "Yes":
            verbose("Stopping IO thread.")
            with conn.cursor() as cursor:
                sql = "STOP SLAVE IO_THREAD"
                cursor.execute(sql)
            cursor.close()
        else:
            verbose("IO thread was already stopped.")
        if slave_sql_running == "Yes":
            verbose("Giving the SQL thread 10 seconds to catch up.")
            sleep(10)
            verbose("Stopping SQL thread.")
            with conn.cursor() as cursor:
                sql = "STOP SLAVE SQL_thread"
                cursor.execute(sql)
            cursor.close()
        else:
            verbose("SQL thread was already stopped.")
        if slave_io_running == "No" and slave_sql_running == "No":
            warn("Replication was already stopped.")


    def mysql_start_replica_single_thread():
        warn("Restarting replication. There was either a problem or you aborted.")
        mysql_query("START SLAVE")


    def mysql_check_long_transactions(is_replica):
        info("Checking for long running transactions.")
        with conn.cursor() as cursor:
            sql = "SELECT 1 FROM information_schema.innodb_trx JOIN information_schema.processlist ON " \
                  "innodb_trx.trx_mysql_thread_id = processlist.id WHERE (NOW() - trx_started) > 60 ORDER BY trx_started "
            cursor.execute(sql)
            result = cursor.fetchone()
        cursor.close()
        if result:
            with conn.cursor() as cursor:
                sql = "SELECT trx_id, trx_started, (NOW() - trx_started) trx_duration_seconds, id processlist_id, user, " \
                      "IF(LEFT(HOST, (LOCATE(':', host) - 1)) = '', host, LEFT(HOST, (LOCATE(':', host) - 1))) host, " \
                      "command, time, REPLACE(SUBSTRING(info,1,25),'\n','') info_25 FROM information_schema.innodb_trx " \
                      "JOIN information_schema.processlist ON innodb_trx.trx_mysql_thread_id = processlist.id WHERE (NOW(" \
                      ") - trx_started) > 60 ORDER BY trx_started "
                cursor.execute(sql)
                result = cursor.fetchall()
                columns = cursor.description
            cursor.close()
            x = PrettyTable([columns[0][0], columns[1][0], columns[2][0], columns[3][0], columns[4][0], columns[5][0],
                             columns[6][0], columns[7][0], columns[8][0]])
            for row in result:
                x.add_row([row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]])
            print(x)
            if is_replica:
                mysql_start_replica_single_thread()
            error(
                "Transaction(s) found running > 60 seconds. COMMIT, ROLLBACK, or kill them. Otherwise, use the less safe --no-transaction-check.")
        else:
            verbose("There are no transactions running > 60 seconds.")


    def mysql_set_dirty_pages_pct(count):
        info("Setting innodb_max_dirty_pages_pct to %s." % count)
        mysql_query("SET GLOBAL innodb_max_dirty_pages_pct = %s" % count)


    def mysql_check_dirty_pages(dirty_pages_start):
        verbose("Checking dirty pages. The starting count is %s." % dirty_pages_start)
        timeout = time() + 60
        while True:
            dirty_pages_current = int(mysql_get_status_variable("Innodb_buffer_pool_pages_dirty"))
            if dirty_pages_current == 0:
                info("Dirty pages is 0.")
                break
            elif dirty_pages_current < (int(dirty_pages_start) * .10):
                verbose("Dirty pages is %s." % dirty_pages_current)
                info("Dirty pages < 10% of the starting count.")
                break
            elif int(dirty_pages_current) > 500:
                verbose("Dirty pages is %s." % dirty_pages_current)
                info("Dirty pages < 500.")
                break
            elif time() > timeout:
                warn("Dirty pages is %s, and did not reach < 10 pct of the starting count after 1 minute."
                     % dirty_pages_current)
                break
            else:
                info("Dirty pages is %s, waiting (up to 1 minute) for it to get lower." % dirty_pages_current)
                sleep(1)


    def mysql_set_fast_shutdown():
        info("Setting innodb_fast_shutdown to 0.")
        mysql_query("SET GLOBAL innodb_fast_shutdown = 0")


    def mysql_set_buffer_pool_dump():
        info("Setting innodb_buffer_pool_dump_at_shutdown to ON.")
        mysql_query("SET GLOBAL innodb_buffer_pool_dump_at_shutdown = ON")
        info("Setting innodb_buffer_pool_dump_pct to 75.")
        mysql_query("SET GLOBAL innodb_buffer_pool_dump_pct = 75")
        buffer_pool_load = mysql_get_global_variable("innodb_buffer_pool_load_at_startup")
        if buffer_pool_load != "ON":
            warn(
                "innodb_buffer_pool_load_at_startup is not enabled. You may want to set this in the my.cnf: innodb_buffer_pool_load_at_startup = ON")

