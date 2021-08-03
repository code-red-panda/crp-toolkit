from time import gmtime, strftime

class Logger:

    def __init__(self, verbose_info):
        self.verbose_info = verbose_info

    def info(self, message, flag="INFO"):
        datetime = strftime("%Y-%m-%d %H:%M:%S", gmtime())
        print(f"{datetime} >>> [ {flag} ] {message}")

    def verbose(self, message, flag="INFO"):
        if self.verbose_info:
            datetime = strftime("%Y-%m-%d %H:%M:%S", gmtime())
            print(f"{datetime} >>> [ {flag} ] {message}")

    def warn(self, message):
        datetime = strftime("%Y-%m-%d %H:%M:%S", gmtime())
        print(f"{datetime} >>> [ WARNING ] {message}")

    def error(self, message):
        datetime = strftime("%Y-%m-%d %H:%M:%S", gmtime())
        print(f"{datetime} >>> [ CRITICAL ] {message}")
        exit(1)
