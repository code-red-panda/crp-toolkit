from time import gmtime, strftime


class Logger:

    def __init__(self, verbose_info):
        self.verbose_info = verbose_info

    @staticmethod
    def info(message, flag="INFO"):
        datetime = strftime("%Y-%m-%d %H:%M:%S", gmtime())
        print(f"{datetime} >>> [ {flag} ] {message}")

    def verbose(self, message, flag="INFO"):
        if self.verbose_info:
            datetime = strftime("%Y-%m-%d %H:%M:%S", gmtime())
            print(f"{datetime} >>> [ {flag} ] {message}")

    @staticmethod
    def warn(message):
        datetime = strftime("%Y-%m-%d %H:%M:%S", gmtime())
        print(f"{datetime} >>> [ WARNING ] {message}")

    @staticmethod
    def error(message):
        datetime = strftime("%Y-%m-%d %H:%M:%S", gmtime())
        print(f"{datetime} >>> [ CRITICAL ] {message}")
        exit(1)
