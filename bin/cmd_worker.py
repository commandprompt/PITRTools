#!/usr/bin/env python
#
# Base class for CMDStandby and CMDArchiver.

import os
import sys
import time
import traceback
import subprocess
from optparse import *
from ConfigParser import *

class CMDWorker(object):
    """
    Base class for CMDArchiver and CMDStandby,
    containing common routines to read configuration options,
    notify external programs and do basic sanity checks.
    """

    def __init__(self, classdict):
        self.classdict = classdict
        self.pitr_bin_path = os.path.realpath(
            os.path.join(os.getcwd(), os.path.dirname(__file__))
        )

    def parse_commandline_arguments(self, argslist, options_check_cb=None,
        usage="usage: %prog [options] arg1 arg2", version="%prog (pitrtools) 1.3\n\nCopyright Command Prompt, Inc.\n\nFor licensing information see the LICENSE file.\n"):

        parser = OptionParser(usage=usage,version=version)

        for arg in argslist:
            parser.add_option(arg[0], arg[1], **arg[2])

        self.options, self.args = parser.parse_args()
        if options_check_cb:
            options_check_cb(parser, self.options)

    def load_configuration_file(self, set_defaults_cb=None):
        result = dict()
        config = ConfigParser()
        files = config.read(self.options.configfilename)
        if not files:
            raise Exception('Configuration file %s is empty or not found' % (self.options.configfilename,))
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
        if set_defaults_cb:
            set_defaults_cb(result)
        self.__dict__.update(result)

        self.check_config()
        self.locate_binaries()

    # checks config values and sets some common defaults
    def check_config(self):
        # set up our ssh transfer timeout and debug options
        self.ssh_flags = "-o ConnectTimeout=%s -o StrictHostKeyChecking=no" % (self.ssh_timeout,)
        if self.ssh_debug:
            self.ssh_flags += " -vvv"

        if 'slaves' in self.__dict__:
            self.slaves_list = self.slaves.replace(" ", "").split(",")
            if not any(self.slaves_list):
                raise Exception("Refusing to run with empty or invalid slaves list.")

    @staticmethod
    def check_paths(pathvars):
        for element in pathvars:
            os.stat(element)

    COMMON_BIN_NAMES = ["rsync", "ssh"]

    #Get and set the required absolute paths for executables
    def locate_binaries(self, exes=COMMON_BIN_NAMES):
        found = []
        exe_paths = []
        final_paths = {}

        #Generator yielding joined paths of directories and filenames [used for searching]
        def search(dirs, names):
            for f in names:
                for directory in dirs:
                    abspath = os.path.join(directory, f)
                    yield f,abspath

        path = []
        if "PATH" in os.environ:
            envpath = os.environ['PATH'].split(os.pathsep)
            path.extend(envpath)
        if 'includepath' in vars(self):
            includepath = self.includepath.split(os.pathsep)
            if path:
                unique = set(includepath).difference(set(envpath))
                path.extend(unique)
            else:
                path.extend(includepath)
        if not path:
            raise Exception("CONFIG: No PATH in environment, and includepath not set in config. Can't find executables.")

        #Start searching
        for exe,abspath in search(path, exes):
            if os.access(abspath, os.X_OK) and exe not in found:
                exe_paths.append(abspath)
                found.append(exe)

        #Raise exception if we couldn't find all the executables
        if len(exes) > len(found):
            raise Exception("CONFIG: Couldn't find executables: %s" % (", ".join(set(exes).difference(set(found)))))

        #Populate final dict of names to paths, assign to self
        for i, exe in enumerate(found):
            final_paths[exe] = exe_paths[i]
        self.__dict__.update(final_paths)

    def pull_exception(self):
        exc = sys.exc_info()
        return traceback.format_exc(exc[2])

    def log(self, msg, level="NOTICE"):
        """
        Log a message to stdout in the format:
        [month.day.year hour:minute:second] level: message

        Arguments:
        | argument | type   | default | description
        * msg      - string -         - Message to log
        * level    - string - NOTICE  - Log level to prepend to message
        """

        timestamp = time.strftime("%F %T %Z")
        print "[%s] %s: %s" % (timestamp, level, msg)
        sys.stdout.flush()  # in case we've been running under logging collector

    def debuglog(self, msg):
        if self.debug:
            self.log(msg, "DEBUG")

    def notify_external(self, log=False, ok=False, warning=False, critical=False, message=None):
        """
        Notify some external program (i.e. monitoring plugin)
        about an event occuring. The program itself can be set
        via notify_* configuration options. 

        Arguments:
        | argument                     | type    | default | description
        * log                          - boolean - False   - Log the message with self.log if true.
        * ok, false, warning, critical - boolean - False   - If one is not set True, immediately return.
        * message                      - string  - None    - Will be appended to the end of the command. 
        """

        #Return if we don't have an alert status
        if not any((ok, warning, critical)):
            return
        if log and message:
            self.log(message)
        #Return if none of the notify commands are set in the config, but not before logging message
        if not filter(len, [self.notify_ok, self.notify_warning, self.notify_critical]):
            return
        if ok:
            exec_str = "%s" % (self.notify_ok,)
        elif warning:
            exec_str = "%s" % (self.notify_warning,)
        elif critical:
            exec_str = "%s" % (self.notify_critical,)
        if message:
            exec_str += " %s" % (message,)

        self.debuglog("notify_external exec_str: %s" % exec_str)
        subprocess.call(exec_str, shell=True)

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


if __name__ == '__main__':
    argslist = [("-C", "--config",
                 dict(dest="configfilename",
                      action="store",
                      help="the name of the archiver config file",
                      metavar="FILE"))]

    # test common config parameters
    classdict = (('rsync_flags', 's', ""),
                ('user', 's', None),
                ('ssh_timeout', 'i', None),
                ('notify_ok', 's', None),
                ('notify_warning', 's', None),
                ('notify_critical', 's', None),
                ('debug', 'b', False),
                ('pgdata', 's', None),
                ('pgcontroldata', 's', ""),
                ('includepath', 's', None),
                ('ssh_debug', 'b', False))

    worker = CMDWorker(classdict)
    worker.parse_commandline_arguments(argslist)
    if worker.options.configfilename:
        worker.load_configuration_file()
    print worker.__dict__
