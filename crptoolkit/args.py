import configparser
import getpass
from os.path import os, expanduser, exists

class ArgParser:

    def __init__(self, args):
        self.args = args

    def connect(self):
        """
        Connection priority:
        1. Provided defaults file
        2. Command line arguments
        3. ~/.my.cnf
        """
        defaults_file = None
        host = '127.0.0.1'
        port = 3306
        user = None
        password = None
        socket = None
        if self.args.defaults_file:
            defaults_file = self.args.defaults_file
            return defaults_file, host, port, user, password, socket
        dot_my_cnf = os.path.expanduser("~/.my.cnf")
        has_dot_my_cnf = True if exists(dot_my_cnf) else False
        if has_dot_my_cnf:
            parser = configparser.ConfigParser()
            parser.read(dot_my_cnf)
        if self.args.host:
            host = self.args.host
        elif has_dot_my_cnf:
            try:
                host = parser.get('client', 'host')
            except:
                pass
        if self.args.user:
            user = self.args.user
        elif has_dot_my_cnf:
            try:
                user = parser.get('client', 'user')
            except:
                pass
        if self.args.ask_pass:
            password = getpass.getpass()
        elif self.args.password:
            password = self.args.password
        elif has_dot_my_cnf:
            try:
                password = parser.get('client', 'password')
            except:
                pass
        if self.args.socket:
            socket = self.args.socket
        elif has_dot_my_cnf:
            try:
                socket = parser.get('client', 'socket')
            except:
                pass
        return defaults_file, host, port, user, password, socket
