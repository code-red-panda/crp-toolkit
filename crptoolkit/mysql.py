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
        """
        Attempts to connect to MySQL
        Returns boolean,error message (if any)
        """
        if self.defaults_file is not None:
            try:
                self.connection = pymysql.connect(
                        read_default_file=self.defaults_file
                        )
                return True, None
            except pymysql.Error as error:
                message = f"{error.args[0]}: {error.args[1]}"
                return False, message
        if self.socket is not None:
            try:
                self.connection = pymysql.connect(
                    host = self.host,
                    port = self.port,
                    user = self.user,
                    password = self.password,
                    unix_socket = self.socket
                    )
                return True, None
            except pymysql.Error as error:
                message = f"{error.args[0]}: {error.args[1]}"
                return False, message
        try:
            self.connection = pymysql.connect(
                    host = self.host,
                    port = self.port,
                    user = self.user,
                    password = self.password
                    )
            return True, None
        except pymysql.Error as error:
            message = f"{error.args[0]}: {error.args[1]}"
            return False, message

    def close(self):
        pymysql.close()

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

    def set_variable(self, variable_name, variable_value, persist=False):
        """
        Sets value of a MySQL variable
        Returns boolean
        """
        sql = f"SET GLOBAL {variable_name} = {variable_value}"
        self.run_query(sql)
        if persist:
            sql = f"SET PERSIST {variable_name} = {variable_value}"
            self.run_query(sql)

    def get_status_variable(self, variable_name):
        """
        Gets value of a MySQL status variable
        Returns value
        """
        sql = f"SHOW GLOBAL STATUS WHERE VARIABLE_NAME = '{variable_name}'"
        result = self.run_query(sql)
        return result[0]['Value']

    def replication_status(self):
        sql = "SHOW REPLICA STATUS"
        return self.run_query(sql)

    def is_replica(self):
        """
        Checks show replica status
        Returns boolean
        """
        is_replica = False if not self.replication_status() else True
        return is_replica

    def stop_replication_io_thread(self):
        sql = "STOP REPLICA IO_thread"
        self.run_query(sql)

    def stop_replication_sql_thread(self):
        sql = "STOP REPLICA SQL_thread"
        self.run_query(sql)

    def stop_replication_mtr(self):
        #STOP SLAVE;
        #START SLAVE UNTIL SQL_AFTER_MTS_GAPS;
        #STOP SLAVE;
        return None

    def start_replication(self):
        sql = ("START REPLICA")
        self.run_query(sql)

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