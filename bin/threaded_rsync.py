#!/usr/bin/env python

"""
Uses rsync to sync files in parallel between a source and destination.

This script has been tested with Python 2.7.4 and 2.6.8; it requires 2.5+.
This script has also been tested with rsync versions 3.0.9 and 2.6.8.


BASIC USAGE:

Using rsync's archive mode along with --delete:
threaded_rsync.py "/usr/bin/rsync -a --delete /path/to/src/ user@host:/path/to/dest"

Specifying the number of threads to spawn for file copies (2 is the default),
and debug mode (default is off):
threaded_rsync.py "/usr/bin/rsync -a /path/to/src/ user@host:/path/to/dest" --num_threads 4 --debug

We are not restricted to archive mode; using other sets of rsync flags:
threaded_rsync.py "/usr/bin/rsync -rltv /path/to/src/ user@host:/path/to/dest"


The script always uses your full rsync command that is passed in (although it
may add additional arguments for certain phases of processing), so --dry-run
could not result in any changes to the source and destination when using threaded_rsync.py:
threaded_rsync.py "/usr/bin/rsync -a --delete --dry-run -vv /path/to/src/ user@host:/path/to/dest"


LIMITING CPU AND DISK USAGE:

Local Host
Use custom nice and ionice settings to limit the cpu and disk contention on the
local host; an example of setting the priority very low:
threaded_rsync.py "nice -n 19 ionice -c 2 -n 7 /usr/bin/rsync -a /path/to/src/ user@host:/path/to/dest"

Remote Host
Similarly, you can prepend nice and ionice calls to rsync's --rsync-path option
to change the priority of the remote host's rsync processes:
threaded_rsync.py 'nice /usr/bin/rsync --rsync-path="nice /usr/bin/rsync" -a /path/to/src/ user@host:/path/to/dest'


BASIC ALGORITHMIC DESCRIPTION:

This script generates an initial list of files to sync by using a user supplied
rsync command. In the first phase --dry-run and --itemize-changes are added to
this rsync command to find a list of files for parallel syncing. In phase two
we sync each of these files individually using a different thread (the caller
chooses how many threads to use). In the final phase we use the original rsync
command again, but we exclude the files that were already synced; directories,
symlinks, etc. are synced in this final (non-threaded) call.
"""

import optparse, re, subprocess, sys, tempfile, thread, time
from threading import Thread
from Queue import Queue

class rsync_in_parallel(object):
    """Main class for managing parallel rsyncs"""

    def __init__(self, rsync_cmd, num_threads=2, debug=False):
        """arguments are:
        the user's rsync command,
        the number of threads to spawn for file transfers (default=2),
        and a flag to show debug information (default=False)"""
        self.rsync_cmd = rsync_cmd
        self.num_threads = num_threads
        self.debug = debug
        self._initialize_file_transfer_list()

        self.queue = Queue()

        for i in range(self.num_threads):
            worker = Thread(target=self._launcher, args=(i,))
            worker.setDaemon(True)
            worker.start()


    def _initialize_file_transfer_list(self):
        """This method constructs a list of files for (later) parallel transfer"""

        # we run the user's rsync command, but we add two flags:
        #   --dry-run --itemize-changes
        # this allows us to find files that need to be transferred
        p = subprocess.Popen(self.rsync_cmd + " --dry-run --itemize-changes", shell=True, stdout=subprocess.PIPE)
        out = p.stdout.readlines()
        # see the rsync man page docs for a complete description of the --itemize-changes output
        # to make sense of the regular expression below; we are looking to transfer files
        # ('f' in the second column below). we will tranfer dirs, etc. later, and all at once.
        # rsync 3.09 uses 11 characters for -i output: YXcstpoguax
        # rsync 2.68 uses  9 characters for -i output: YXcstpogz
        re_obj = re.compile(r"^[<>ch.]f[c.+][s.+][tT.+][p.+][o.+][g.+][uz.+][a.+]?[x.+]?\s(?P<file_name>.+)$")

        # a list of all files for parallel/threaded sync
        self.file_list = []
        for line in out:
            #print "LINE:" + line
            match = re_obj.match(line.strip())

            if (match):
                file_path = match.groupdict()['file_name']
                self.file_list.append('/' + file_path)
                #print "MATCH:" + file_path

        if len(self.file_list) == 0:
            print "WARN: no files will be transferred in parallel; check the output of --dry-run --itemize-changes with your rsync command to verify"

    def _launcher(self, i):
        """Spawns an rsync process to update/sync a single file"""
        while True:
            file_path = self.queue.get()
            if self.debug:
                print "Thread %s: %s" % (i, file_path)

            # take the users's rsync command but use --files-from to just send a specific file
            # (parent directories of the file will be created automatically if they are needed)
            temp = tempfile.NamedTemporaryFile()
            temp.write(file_path)
            temp.flush()

            cmd = "%s --files-from=%s" % (self.rsync_cmd, temp.name)
            if self.debug:
                print "CALLING:" + cmd

            ret = subprocess.call(cmd, shell=True)
            if ret != 0:
                print "WARN: could not transfer %s, rsync failed with error code=%s; continuing..." % (file_path, ret)

            temp.close()
            self.queue.task_done()


    def sync_files(self):
        """The main entry point to start the sync processes"""

        # create a (synchronized) queue for the threads to access
        for file_path in self.file_list:
            self.queue.put(file_path)
        self.queue.join()

        # now we perform the final call to rsync to sync directories, symlinks,
        # perform deletes (if --delete was in the original command), etc.
        # i.e., everything that remains beyond the parallel file transfers
        # that have already occurred.

        # we could just issue the original command, but it will be faster to
        # explicitly --exclude-from the files we already transferred (especially
        # when --checksum is used in the original command)
        temp = tempfile.NamedTemporaryFile()
        for file_path in self.file_list:
            temp.write(file_path + "\n")
        temp.flush()
        cmd = "%s --exclude-from=%s" % (self.rsync_cmd, temp.name)

        if (self.debug):
            print "Calling final rsync:" + cmd
        ret = subprocess.call(cmd, shell=True)
        if ret != 0:
            print "WARN: potential problem with final rsync call, rsync failed with error code=%s" % ret

        temp.close()
        return ret


if __name__ == "__main__":
    p = optparse.OptionParser(description="Python threaded rsync",
        prog="threaded_rsync.py",
        version="0.1",
        usage="%prog <rsync command>")

    p.add_option('--num_threads', '-n', type="int", help="the number of spawned rsync file copy threads", default=2)
    p.add_option('--debug', '-d', help="enable debugging output", action="store_true", default=False)

    options, arguments = p.parse_args()

    if options.debug:
        print arguments
        print options

    if len(arguments) != 1:
        #print __doc__
        p.print_help()
        sys.exit(1)

    #start = time.time()

    rsync_cmd = arguments[0]
    r = rsync_in_parallel(rsync_cmd, options.num_threads, options.debug)
    ret = r.sync_files()

    #end = time.time()
    #print "rsyncs completed in %s seconds" % (end - start)

    sys.exit(ret)
