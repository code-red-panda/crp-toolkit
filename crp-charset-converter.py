#!/usr/bin/env python

import argparse
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
    parser.add_argument("--defaults-file", type=str, dest="defaults_file", metavar="FILE", help="Use MySQL configuration file")
    parser.add_argument("-v", "--verbose", dest="verbose", action="store_true", help="Print additional tool information")
    parser.add_argument("-c", "--charset", type=str, dest="charset", default="utf8mb4", help="Charset to convert to (default: utf8mb4)")
    parser.add_argument("-l", "--collation", type=str, dest="collation", default="utf8mb4_0900_ai_ci", help="Collation to convert to (default: utf8mb4_0900_ai_ci)")
    parser.add_argument("--no-preflight", action="store_true", dest="no_preflight", help="Do not perform preflight checks")
    parser.add_argument("--no-ddl", action="store_true", dest="no_ddl", help="Do not generate DDL statements")
    return parser.parse_args()

class MySQLCharsetConversion():

    def __init__(self, hostname):
        self.args = ArgParser(args)
        self.no_ddl = args.no_ddl
        self.no_preflight = args.no_preflight
        self.charset = args.charset
        self.collation = args.collation
        self.verbose = args.verbose
        self.defaults_file, self.host, self.port, self.user, self.password, self.socket = self.args.connect()
        self.mysql = MySQL(self.defaults_file, self.host, self.port, self.user, self.password, self.socket)
        self.connect = self.mysql.connect()
        self.log = Logger(self.verbose)

    def validate(self):
        sql = f"SELECT 1 FROM information_schema.collation_character_set_applicability WHERE character_set_name='{self.charset}' AND collation_name='{self.collation}';"
        valid = self.mysql.run_query(sql, selectone=True)
        if not valid:
            self.log.warn(sql)
            self.log.error(f"The character set and/or collation are not configured in MySQL or they are an incompatible combination.")
        return valid

    def preflight(self):
        queries = {
                1: {
                    "info": "Character set and collation global variables",
                    "query": "SHOW GLOBAL VARIABLES WHERE variable_name IN ('innodb_file_format', 'innodb_large_prefix', 'character_set_client', 'character_set_connection', 'character_set_database', 'character_set_results','character_set_server', 'collation_connection', 'collation_database', 'collation_server', 'default_collation_for_utf8mb4');"
                    },
                2: {
                    "info": f"Databases that are not {self.charset} and {self.collation}",
                    "query": f"SELECT schema_name, default_character_set_name, default_collation_name FROM information_schema.schemata WHERE schema_name NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys') AND (default_character_set_name != '{self.charset}' OR default_collation_name != '{self.collation}');"
                    },
                3: {
                    "info": f"Table that are not {self.charset} and {self.collation}",
                    "query": f"SELECT t.table_schema, t.table_name, c.character_set_name, t.table_collation FROM information_schema.tables t JOIN information_schema.collation_character_set_applicability c ON c.collation_name = t.table_collation WHERE t.table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys') AND (c.character_set_name != '{self.charset}' OR t.table_collation != '{self.collation}');"
                    },
                4: {
                    "info": "Client connections overriding character set and collation session variables",
                    "query": "SELECT threads.* FROM ( SELECT t.processlist_user, t.processlist_db, v.variable_name, v.variable_value FROM performance_schema.threads AS t JOIN performance_schema.variables_by_thread AS v ON v.thread_id = t.thread_id WHERE t.processlist_user IS NOT NULL AND v.variable_name LIKE 'character_set_%' OR v.variable_name LIKE 'collation_%') threads JOIN ( SELECT variable_name, variable_value FROM performance_schema.global_variables WHERE variable_name LIKE 'character_set_%' OR variable_name LIKE 'collation_%') vars ON threads.variable_name = vars.variable_name WHERE threads.variable_value != vars.variable_value AND threads.processlist_db NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys') GROUP BY threads.processlist_user, threads.processlist_db, threads.variable_name ORDER BY threads.processlist_user, threads.processlist_db;"
                    },
                5: {
                    "info": "Indexed string columns > 3072 bytes (for utf8 -> utf8mb4 conversions)",
                    "query": "SELECT c.table_schema, c.table_name, c.column_name, c.character_set_name column_character_set, CONCAT(c.data_type, '(', c.character_maximum_length, ')') data_type, IF(s.sub_part IS NULL, (c.character_maximum_length * 4), (s.sub_part * 4)) index_prefix_length, s.index_name, s.index_type, s.sub_part index_sub_part FROM information_schema.columns c JOIN information_schema.statistics s ON c.table_name = s.table_name AND c.column_name = s.column_name WHERE c.table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys') AND (c.data_type LIKE '%TEXT%' OR c.data_type LIKE '%CHAR%' OR c.data_type LIKE '%text%' OR c.data_type LIKE '%char%') AND s.index_type <> 'FULLTEXT' AND c.character_set_name <> 'utf8mb4' HAVING index_prefix_length > 3072 ORDER BY index_prefix_length DESC;"
                    },
                6: {
                    "info": "Foreign Key string columns",
                    "query": "SELECT k.table_name, k.column_name, k.referenced_table_name, k.referenced_column_name, c.data_type FROM information_schema.key_column_usage k JOIN information_schema.columns c ON k.referenced_table_schema = c.table_schema AND k.referenced_table_name = c.table_name AND k.referenced_column_name = c.column_name WHERE k.referenced_table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys') AND (c.data_type LIKE '%TEXT%' OR c.data_type LIKE '%CHAR%' OR c.data_type LIKE '%text%' OR c.data_type LIKE '%char%');"
                    }
                }

        for key, value in sorted(queries.items()):
            self.log.newline()
            self.log.no_timestamp(f"{key}) {value['info']}:")
            self.log.verbose(value["query"])
            self.log.no_timestamp(self.mysql.run_query(value["query"], prettytable=True))

    def ddl(self):
        queries = {
                7: {
                    "info": "Persist collation variables in the my.cnf",
                    "query": f"SELECT CONCAT(variable_name, ' = {self.collation}') cmd FROM performance_schema.global_variables WHERE variable_name IN ('collation_server', 'default_collation_for_utf8mb4');"
                    },
                8: {
                    "info": "Perist character set variables in the my.cnf",
                    "query": f"SELECT CONCAT(variable_name, ' = {self.charset}') cmd FROM performance_schema.global_variables WHERE variable_name IN ('character_set_server');"
                    },
                9: {
                    "info": f"Configure runtime collation global variables to {self.collation}",
                    "query": f"SELECT CONCAT('SET GLOBAL ', variable_name, ' = \"{self.collation}\";') cmd FROM performance_schema.global_variables WHERE variable_name IN ('collation_connection', 'collation_database', 'collation_server', 'default_collation_for_utf8mb4') AND variable_value != '{self.collation}';"
                    },
                9: {
                    "info": f"Configure runtime character set global variables to {self.charset}",
                    "query": f"SELECT CONCAT('SET GLOBAL ', variable_name, ' = \"{self.charset}\";') cmd FROM performance_schema.global_variables WHERE variable_name IN ('character_set_client', 'character_set_connection', 'character_set_database', 'character_set_results', 'character_set_server') AND variable_value != '{self.charset}';"
                    },
                10: {
                    "info": f"DDL to alter databases to {self.charset} and {self.collation}",
                    "query": f"SELECT CONCAT ('ALTER DATABASE `', schema_name, '` DEFAULT CHARACTER SET {self.charset} COLLATE {self.collation};') ddl FROM information_schema.schemata WHERE schema_name NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys') AND (default_character_set_name != '{self.charset}' OR default_collation_name != '{self.collation}');"
                    },
                11: {
                    "info": f"DDL to alter tables <= 1G to {self.charset} and {self.collation}",
                    "query": f"SELECT CONCAT('ALTER TABLE `', t.table_schema, '`.`', t.table_name,'` CONVERT TO CHARACTER SET {self.charset} COLLATE {self.collation};') ddl FROM information_schema.tables t JOIN information_schema.collation_character_set_applicability c ON c.collation_name = t.table_collation WHERE t.table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys') AND ROUND(((t.data_length + t.index_length)/1024/1024),0) <= 1024 AND (c.character_set_name != '{self.charset}' OR t.table_collation != '{self.collation}');"
                    },
                12: {
                    "info": f"pt-online-schema-change DDL to alter tables > 1G to {self.charset} and {self.collation}",
                    "query": "SELECT CONCAT('pt-online-schema-change D=', t.table_schema, ',t=', t.table_name, ' --host=__source__ --alter=CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci') ddl FROM information_schema.tables t JOIN information_schema.collation_character_set_applicability c ON c.collation_name = t.table_collation WHERE t.table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys') AND ROUND(((t.data_length + t.index_length)/1024/1024),0) > 1024 AND (c.character_set_name != '{self.charset}' OR t.table_collation != '{self.collation}');"
                    }
                }

        for key, value in sorted(queries.items()):
            self.log.newline()
            self.log.no_timestamp(f"{key}) {value['info']}:")
            self.log.verbose(value["query"])
            result = self.mysql.run_query(value["query"])
            if type(result) == list:
                for row in result:
                    for value in row.values():
                        self.log.no_timestamp(value)
            if type(result) == str:
                self.log.no_timestamp(result)

    def run(self):
        # Persist sql
        self.log.info(f"[ START ] Running MySQL character set conversion for: {self.charset} and {self.collation}")
        self.validate()
        if not self.no_preflight:
            self.log.info("Performing preflight checks...")
            self.preflight()
        if not self.no_ddl:
            self.log.newline()
            self.log.info("Generating commands and DDL statements...")
            self.log.warn("Only run the following commands if you are sure the database is ready!")
            self.ddl()
        self.log.newline()
        self.log.info(f"[ COMPLETED ]")


if __name__ == "__main__":
    args = args()
    MySQLCharsetConversion(args).run()
