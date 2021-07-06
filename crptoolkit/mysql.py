import pymysql.cursors
from prettytable import PrettyTable

class MySQL:

    def __init__(self, defaults_file, host, port, user, password, socket):
        self.connection = None
        self.defaults_file = defaults_file
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.socket = socket

    def connect(self):
        if self.defaults_file is not None:
            self.connection = pymysql.connect(
                    read_default_file=self.defaults_file
                    )
            return self.connection
        if self.socket is not None:
            self.connection = pymysql.connect(
                    host = self.host,
                    port = self.port,
                    user = self.user,
                    password = self.password,
                    unix_socket = self.socket
                    )
            return self.connection
        self.connection = pymysql.connect(
                host = self.host,
                port = self.port,
                user = self.user,
                password = self.password
                )
        return self.connection

    def run_query(self, sql, cursorclass = pymysql.cursors.DictCursor):
        """
        Executes SQL and retreives result
        Returns all rows (formatted by cursor class)
        """
        with self.connection.cursor(cursorclass) as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
        cursor.close()
        return result

    def get_variable(self, variable_name):
        """
        Gets value of a MySQL variable
        Returns value
        """
        sql = f"SHOW GLOBAL VARIABLES WHERE VARIABLE_NAME = '{variable_name}'"
        result = self.run_query(sql)
        return result[0]['Value']

    def set_variable(self, variable_name, variable_value):
        """
        Sets value of a MySQL variable
        Returns boolean
        """
        sql = f"SET GLOBAL {variable_name} = {variable_value}"
        self.run_query(sql)

    def get_status_variable(self, variable_name):
        """
        Gets value of a MySQL status variable
        Returns value
        """
        sql = f"SHOW GLOBAL STATUS WHERE VARIABLE_NAME = '{variable_name}'"
        result = self.run_query(sql)
        return result[0]['Value']

    def is_replica(self):
        """
        Checks show slave status
        Returns boolean
        """
        sql = "SHOW SLAVE STATUS"
        if not self.run_query(sql):
            return False
        return True

    def replica_status(self):
        print('hi')

    def stop_replication(self):
        sql = "SHOW SLAVE STATUS"
        result = self.run_query(sql)
        if (slave_io_running := result["Slave_IO_Running"]) == "Yes":
            sql = "STOP SLAVE IO_THREAD"
            self.run_query(sql)
        else:
            verbose("IO thread was already stopped.")
        if (slave_sql_running := result["Slave_SQL_Running"]) == "Yes":
            verbose("Giving the SQL thread 10 seconds to catch up.")
            sleep(10)
            verbose("Stopping SQL thread.")
            sql = "STOP SLAVE SQL_thread"
            self.run_query(sql)
        else:
            verbose("SQL thread was already stopped.")
        if slave_io_running == "No" and slave_sql_running == "No":
            warn("Replication was already stopped.")

    def start_replication(self):
        sql = ("START SLAVE")
        self.run_query(sql)
        # What to return?

    def get_transactions(self, duration):
        """
        Gets MySQL transactions running > {duration} seconds
        Returns pretty printed table
        """
        sql = f"SELECT trx_id, trx_started, (NOW() - trx_started) trx_duration_seconds, id processlist_id, user, IF(LEFT(HOST, (LOCATE(':', host) - 1)) = '', host, LEFT(HOST, (LOCATE(':', host) - 1))) host, command, time, REPLACE(SUBSTRING(info,1,25),'\n','') info_25 FROM information_schema.innodb_trx JOIN information_schema.processlist ON innodb_trx.trx_mysql_thread_id = processlist.id WHERE (NOW() - trx_started) > {duration} ORDER BY trx_started"
        if not (result := self.run_query(sql)):
            return False
        else:
            table = PrettyTable(result[0].keys(), align = "l")
            for rows in result:
                table.add_row(list(rows.values()))
            print(table)
            return True
