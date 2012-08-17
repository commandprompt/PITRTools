#!/usr/bin/env python
#
# Base class for CMDStandby and CMDArchiver.


from optparse import *
from ConfigParser import *


class CMDWorker:
    """
    Base class for CMDArchiver and CMDStandby,
    containing common routines to read configuration options,
    notify external programs and do basic sanity checks.
    """

    def __init__(self, classdict):
        self.classdict = classdict

    @staticmethod
    def parse_commandline_arguments(argslist,
        usage="usage: %prog [options] arg1 arg2"):

        parser = OptionParser(usage=usage)
        for arg in argslist:
            parser.add_option(arg[0], arg[1], **arg[2])
        options, args = parser.parse_args()
        return (options, args)

    def load_configuration_file(self, configfilename):
        result = dict()
        config = ConfigParser()
        files = config.read(configfilename)
        if not files:
            raise Exception('Configuration file %s is empty or not found' % (configfilename,))
        for opt in self.classdict:
            key, typ, default = opt
            val = None
            try:
                if typ == 's':
                    val = config.get('DEFAULT', key)
                elif typ == 'b':
                    val = config.getboolean('DEFAULT', key)
                elif typ == 'i':
                    val = config.getint('DEFAULT', key)
                elif typ == 'f':
                    val = config.getfloat('DEFAULT', key)
                else:
                    raise Exception('Invalid type for %s: %s' % (key, typ))
            except NoOptionError:
                if default != None:
                    val = default
                else:
                    raise
            result[key] = val
        self.__dict__.update(result)

    def notify_external(self, ok=False, warning=False, critical=False, message=None):
        """
        Notify some external program (i.e. monitoring plugin)
        about an event occured. The program itself can be set
        via notify_* configuration options.
        """
        if ok:
            exec_str = "%s" % (self.notify_ok,)
        elif warning:
            exec_str = "%s" % (self.notify_warning,)
        elif critical:
            exec_str = "%s" % (self.notify_critical,)
        if message:
            exec_str += " %s" % (message,)
        if ok or warning or critical:
            system(exec_str)

    def check_pgpid_func(self):
        """
        Checks to see if postgresql is running
        """
        if self.debug:
            print "NOTICE: check_pgpid_func()"
        pidfilename = '%s/postmaster.pid' % (self.pgdata,)
        try:
            os.stat(pidfilename)
            pidfile = open(pidfilename, 'r')
            line = int(pidfile.readline())
            os.kill(line, 0)
            return 0
        except:
            return 1

    # set up our ssh transfer timeout and debug options
    def set_ssh_flags(self):
        self.ssh_flags = "-o ConnectTimeout=%s -o StrictHostKeyChecking=no " % (self.ssh_timeout,)
        if self.ssh_debug:
            self.ssh_flags += '-vvv '


if __name__ == '__main__':
    argslist = (('-F', '--file', dict(dest="archivefilename",
                action="store", help="Archive file", metavar="FILE")),
               ("-C", "--config", dict(dest="configfilename",
                action="store",  help="the name of the archiver config file",
                metavar="FILE", default='cmd_archiver.ini')),
               ("-f", "--flush", dict(dest="flush", action="store_true",
                help="Flush all remaining archives to slave")),
               ("-I", "--init", dict(dest="init", action="store_true",
                help="Initialize master environment")))

    classdict = (('state', 's', None),
                ('rsync_bin', 's', None),
                ('rsync_flags', 's', ""),
                ('slaves', 's', None),
                ('user', 's', None),
                ('r_archivedir', 's', None),
                ('l_archivedir', 's', None),
                ('timeout', 'i', None),
                ('notify_ok', 's', None),
                ('notify_warning', 's', None),
                ('notify_critical', 's', None),
                ('debug', 'b', False),
                ('pgdata', 's', None),
                ('pgcontroldata', 's', ""),
                ('rsync_version', 'i', None),
                ('ssh_debug', 'b', False))

    worker = CMDWorker(classdict)
    (options, args) = worker.parse_commandline_arguments(argslist)
    worker.load_configuration_file(options.configfilename)
    print worker.__dict__
