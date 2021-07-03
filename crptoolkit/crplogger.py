class Logger:
    def info(message):
        print(strftime("%Y-%m-%d %H:%M:%S", gmtime()) + " >>> %s" % message)

    def verbose(message):
        if options.verbose:
            print(strftime("%Y-%m-%d %H:%M:%S", gmtime()) + " >>> %s" % message)

    def warn(message):
        print(strftime("%Y-%m-%d %H:%M:%S", gmtime()) + " >>> [ WARNING ] %s" % message)

    def error(message):
        print(strftime("%Y-%m-%d %H:%M:%S", gmtime()) + " >>> [ CRITICAL ] %s" % message)
        exit(1)