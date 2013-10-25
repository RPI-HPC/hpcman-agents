"""Common code for agents."""

__all__ = [
    "AgentError", "Agent", "AgentDatabaseError"
    ]

import ConfigParser
import logging, logging.handlers
from optparse import OptionParser
import os, os.path, sys

from DBHelpers import *
import psycopg2, psycopg2.extras

_hasSSLMode = True

class AgentError(Exception):
    """Error related to HPCMAN agents."""

class AgentDatabaseError(AgentError): pass


class Agent(object):
    """Base class for agents."""

    CONSECT = "connection"

    defer_database_connection = False

    def __init__(self, agentKey):

        # Define options.  Parse and save the results.
        self.define_options()
        parser = self.parser
        (self.options, self.args) = parser.parse_args()

        # Create an configuration file parser.
        self.configParser = cp = ConfigParser.SafeConfigParser()

        # Was an agent key specified?
        self.agentKey = self.options.agentKey
        if self.agentKey is None:
            self.agentKey = agentKey

        # Was a configuration file specified?
        if self.options.configFile is not None:
            fp = open(self.options.configFile, "r")
            cp.readfp(fp)
            fp.close()
        else:
            cfileList = self.get_config_file_list(self.agentKey)
            cp.read(cfileList)

        # Process configuration information.
        self.get_config_data()
        
        # Make sure the state directory exists.
        try:
            os.makedirs(self.stateDir, 0750)
        except:
            pass
        if not os.access(self.stateDir, os.R_OK|os.W_OK):
                logging.fatal("Missing or unwritable state directory: %s",
                              self.stateDir)
                sys.exit(1)

        # Start daemon mode.
        if self.options.daemonFlag:
            pid = self._daemonize()
            if pid != 0:
                sys.stdout.write(str(pid)+"\n")
                sys.stdout.flush()
                os._exit(0)

        # Start logging.
        self.set_logger()

        # Initialize state relating to the database connection.
        self.conn = None
        self.reconnectDBTime = 0

    # -- options --

    def define_options(self):
        """Options pertaining to all agents."""
        parser = self.parser = OptionParser()
        parser.add_option("-k", "--key",
                          action="store", dest="agentKey",
                          metavar="KEY",
                          help="Use KEY as the agent key.")
        parser.add_option("-c", "--config",
                          action="store", dest="configFile",
                          metavar="CONFIGFILE",
                          help="get configuration from CONFIGFILE")
        parser.set_defaults(daemonFlag=False)
        parser.add_option("-d", "--daemon",
                          action="store_true", dest="daemonFlag",
                          help="run this agent as a daemon")

    # -- get configuration data --

    def get_config_file_list(self, agentKey):
        """Get a list of candidate configuration files, based on agent key."""
        cfName = agentKey+".cfg"
        return [cfName,
                os.path.join("/etc", cfName)]

    def get_config_data(self):
        """Get the configuration data common to all agents."""

        self.get_config_data_agent_state()
        self.get_config_data_logging()
        self.get_config_data_database_connection()

    def get_config_data_agent_state(self):
        """Get agent-specific state information."""
        cp = self.configParser
        self.stateDir = cp.get(self.CONSECT, "statedirectory")


    def get_config_data_logging(self):
        """Get agent-specific logging information."""
        cp = self.configParser
        try:
            self.logfile = cp.get(self.CONSECT, "logfile")
        except:
            self.logfile = "STDERR"
        try:
            self.loglevel = cp.get(self.CONSECT, "loglevel").upper()
        except:
            self.loglevel = logging.WARNING
        else:
            try:
                self.loglevel = {"DEBUG": logging.DEBUG,
                                 "INFO": logging.INFO,
                                 "WARN": logging.WARN,
                                 "WARNING": logging.WARN,
                                 "ERROR": logging.ERROR,
                                 "FATAL": logging.CRITICAL,
                                 "CRITICAL": logging.CRITICAL}[self.loglevel]
            except KeyError:
                self.loglevel = logging.WARNING
                logging.error("Invalid log level defined.")

    def get_config_data_database_connection(self):
        """Get configuration information needed to connect to the database."""
        cp = self.configParser
        kw={}
        try:
            kw["host"] = cp.get(self.CONSECT, "host")
        except:
            pass
        try:
            kw["user"] = cp.get(self.CONSECT, "dbuser")
        except:
            pass
        try:
            kw["password"] = cp.get(self.CONSECT, "dbpassword")
        except:
            pass
        try:
            kw["database"] = cp.get(self.CONSECT, "database")
        except:
            pass
        if _hasSSLMode:
            try:
                kw["sslmode"] = cp.get(self.CONSECT, "sslmode")
                #FIXME: Ensure one of: disable, allow, prefer, require.
            except:
                pass

        self.db_connect_info = kw


    # -- logging --

    def set_logger(self):
        """Set up the logger."""
        logfile = self.logfile
        logger = logging.getLogger(self.agentKey)
        if logfile == "STDERR":
            h = logging.StreamHandler()
            f = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        elif logfile == "SYSLOG":
            h = logging.handlers.SysLogHandler()
            f = logging.Formatter("%(levelname)s %(message)s")
        else:
            h = logging.FileHandler(logfile)
            f = logging.Formatter("%(asctime)s %(levelname)s "+
                                  self.agentKey+": %(message)s")
        h.setFormatter(f)
        logger.addHandler(h)
        logger.setLevel(self.loglevel)
        self.logger = logger


    # -- database --

    def get_connection(self):
        """Get or make a connection object to the database."""
        if self.conn is None:
            try:
                conn = DB_connect(**self.db_connect_info)
                conn.set_isolation_level(0)
                self.conn = conn
            except DBDatabaseError:
                self.logger.info("Failed to connect to database.")
                raise
            else:
                self.logger.info("Connected to database")
        return self.conn


    def get_cursor(self):
        """Get a cursor to the database."""

        return self.get_connection().cursor()


    def close_connection(self, **kw):
        """Close a database connection."""

        self.logger.debug("Database connection is closing.")

        # Close cursors, passed as 'cur' or as elements of 'curList'.
        cur = kw.get('cur')
        if cur is not None:
            try:
                cur.close()
            except:
                pass
        curList = kw.get('curList', [])
        for cur in curList:
            try:
                cur.close()
            except:
                pass

        # Close the connection itself.
        if self.conn is not None:
            try:
                conn.close()
            except:
                pass
        self.conn = None


    def db_timestamp(self, year,month,day, hour,minutes,seconds, tzinfo=None):
        """Create a timestamp for the database."""
        return DBTimestamp(year,month,day, hour,minutes,seconds, tzinfo)


    def commit(self):
        """Commit changes to the database."""
        self.get_connection().commit()


    # -- timestamps --        

    def get_timestamp_filename(self, subkey=None):
        """Get the name of a timestamp file."""
        if subkey is None:
            fn = self.agentKey + ".timestamp"
        else:
            fn = self.agentKey + "-" + subkey + ".timestamp"
        return os.path.join(self.stateDir, fn)


    def get_timestamp(self, subkey=None):
        """Get the timestamp from when we last updated things."""

        fname = self.get_timestamp_filename(subkey)
        try:
            f = open(fname, 'r')
            r = f.readline()
            f.close()
        except IOError:
            return DBTimestamp(1900,1,1, 0,0,0)

        return r[:-1]


    def set_timestamp(self, cur, subkey=None, ts=None):
        """Write a timestamp file.

        This is tied to a cursor, since we want database time of the
        current transaction."""

        if ts is None:
            cur.execute("SELECT now();")
            R = cur.fetchall()
            t = R[0][0]
        else:
            t = ts

        fname = self.get_timestamp_filename(subkey)
        f = open(fname, "w")
        f.write(str(t)+"\n")
        f.close()


    # -- service interface --

    def _daemonize(self):
        """Enter daemon mode."""

        r,w = os.pipe()
        pid = os.fork()
        if pid == 0:
            # Child, completed first fork.
            os.setsid()
            os.close(r)
            pid = os.fork()
            if pid == 0:
                # This is the daemon.
                os.chdir(self.stateDir)
            else:
                os._exit(0)
        else:
            # This is the parent.
            os.close(w)
            realPid = int(os.read(r, 1024))
            os.close(r)
            return realPid

        # FIXME: Maybe remove this to a method to call upon completion of
        #        initialization.
        s = str(os.getpid())
        os.write(w, s)
        os.close(w)

        # 
        try:
            maxfd = os.sysconf("SC_OPEN_MAX")
        except (AttributeError, ValueError):
            maxfd = 1024
        for fd in range(maxfd):
            try:
                os.close(fd)
            except:
                pass
        fd = os.open("/dev/null", os.O_RDWR)
        os.dup2(0,1)
        os.dup2(0,2)

        return 0
