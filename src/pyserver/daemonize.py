# #!/usr/bin/python

import fcntl
import os
import sys
import signal
import resource
#import logging
import atexit
import time
#from logging import handlers


class Daemonize(object):
    """ Daemonize object
    Object constructor expects three arguments:
    - app: contains the application name which will be sent to syslog.
    - pid: path to the pidfile.
    - action: your custom function which will be executed after daemonization.
    """
    def __init__(self, app, pidfile, action):
        self.app = app
        self.pidfile = pidfile
        self.action = action

        """
        # Initialize logging.
        self.logger = logging.getLogger(self.app)
        self.logger.setLevel(logging.DEBUG)
        # Display log messages only on defined handlers.
        self.logger.propagate = False

        syslog_address = "/dev/log"
        syslog = handlers.SysLogHandler(syslog_address)
        syslog.setLevel(logging.INFO)
        # Try to mimic to normal syslog messages.
        formatter = logging.Formatter("%(asctime)s %(name)s: %(message)s",
                                      "%b %e %H:%M:%S")
        syslog.setFormatter(formatter)
        self.logger.addHandler(syslog)
        """

    def sigterm(self, signum, frame):
        """ sigterm method
        These actions will be done after SIGTERM.
        """
        #self.logger.warn("Caught signal %s. Stopping daemon." % signum)
        os.remove(self.pidfile)
        sys.exit(0)

    def start(self):
        """ start method
        Main daemonization process.
        """
        def fork_then_exit_parent():
            """
            Fork, creating a new process for the child.
            then exit the parent process.
            """
            process_id = os.fork()
            if process_id < 0:
                # Fork error. Exit badly.
                #self.logger.error("pid < 0.")
                sys.exit(1)
            elif process_id > 0:
                # This is the parent process. Exit.
                sys.exit(0)

        fork_then_exit_parent()

        # Magic two fork process, see Steven's APUE
        fork_then_exit_parent()

        # This is the child process. Continue.
        # Set umask to default to safe file permissions when running as a root daemon
        os.umask(0)

        # setsid puts the process in a new parent group and detaches its controlling terminal.
        os.setsid()

        # Change the root directory of this process.
        #os.chdir("/")

        # Close all file descriptors
        for fd in range(resource.getrlimit(resource.RLIMIT_NOFILE)[0]):
            try:
                os.close(fd)
            except OSError:
                pass

        os.open(os.devnull, os.O_RDWR)

        # Create a lockfile so that only one instance of this daemon is running at any time.
        lockfile = open(self.pidfile, "w")
        # Try to get an exclusive lock on the file. This will fail if another process has the file
        # locked.
        try:
            fcntl.lockf(lockfile, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError:
            #self.logger.warn("Daemon already running, exit process.")
            raise OSError("Daemon already running, exit process.")
            sys.exit(1)

        # Record the process id to the lockfile. This is standard practice for daemons.
        lockfile.write("%s\n" % (os.getpid()))
        #self.logger.warn("write pid %s to tmp.pid file.", os.getpid())
        lockfile.flush()

        # Set custom action on SIGTERM.
        signal.signal(signal.SIGTERM, self.sigterm)
        atexit.register(self.sigterm)
        #self.logger.warn("Starting daemon.")

        self.action()

    def stop(self):
        """
        Stop the daemon
        """
        # Get the pid from the pidfile
        try:
            pf = file(self.pidfile, 'r')
            pid = int(pf.read().strip())
            pf.close()
        except:
            pid = None

        if not pid:
            message = "pidfile %s does not exist. Daemon not running?\n"
            sys.stderr.write(message % self.pidfile)
            return  # not an error in a restart

        # Try killing the daemon process
        try:
            while 1:
                os.kill(pid, signal.SIGTERM)
                time.sleep(0.1)
        except OSError, err:
            err = str(err)
            if err.find("No such process") > 0:
                if os.path.exists(self.pidfile):
                    os.remove(self.pidfile)
            else:
                print str(err)
                sys.exit(1)

    def restart(self):
        """
        Restart the daemon
        """
        self.stop()
        self.start()
