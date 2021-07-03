from time import gmtime, strftime

class Logger:
    def info(self, message):
        print(strftime("%Y-%m-%d %H:%M:%S", gmtime()) + " >>> %s" % message)

    def verbose(self, message):
        if options.verbose:
            print(strftime("%Y-%m-%d %H:%M:%S", gmtime()) + " >>> %s" % message)

    def warn(self, message):
        print(strftime("%Y-%m-%d %H:%M:%S", gmtime()) + " >>> [ WARNING ] %s" % message)

    def error(self, message):
        print(strftime("%Y-%m-%d %H:%M:%S", gmtime()) + " >>> [ CRITICAL ] %s" % message)
        exit(1)