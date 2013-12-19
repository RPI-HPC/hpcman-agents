"""Common code for Site Agents."""

import logging
import os.path
import select
import socket
import subprocess
import sys
import time
import xdrlib

from Agent import *
from DBHelpers import *
from AgentSlave import *
from AgentMaster import *

__all__ = [
    # From SiteAgent
    "VSiteError", "UpdateVSiteError",
    "SlaveError", "SlaveSiteError",
    "SlaveConnectionError", "SlaveProtocolError",
    "VSiteAgent", "SiteAgent",
    # From Agent
    "Agent", "AgentError", "AgentDatabaseError"
    ]

class VSiteError(AgentError): pass

class UpdateVSiteError(VSiteError): pass


class VSiteAgent(object):
    """Model a VSite updating instance."""

    def __init__(self, aHandle, vsName):
        self.vsName = vsName
        self.aHandle = aHandle
        self.is_bootstrapping = False
        self.retryUpdateTime = None
        cp = aHandle.configParser
        try:
            self.updateRetryInterval = cp.getfloat(vsName,
                                                   "update_retry_interval")
        except:
            self.updateRetryInterval = None

    def prepare_update(self, cur):
        self.timeStamp = self.get_timestamp()

    def update_groups(self, cur, end=None):
        pass

    def update_users(self, cur, end=None):
        pass

    def finish_update(self, cur, completed, end=None):

        if completed:
            # FIXME: Slight race -- Connection might die before commit.
            self.set_timestamp(cur)
            self.aHandle.conn.commit()

            self.retryUpdateTime = None
            self.aHandle.notify_vsites(self.vsName)

        else:
            if self.updateRetryInterval is not None:
                self.retryUpdateTime = time.time() + self.updateRetryInterval


    def vsite_update(self, cur, end=None):
        """Do an update of this vsite."""

        aHandle = self.aHandle
        logger = aHandle.logger
        logger.debug("Updating VSite %s", self.vsName)

        # Do prepare_update.  If we succeed, we will need to finish_update.
        try:
            self.prepare_update(cur)
        except DBDatabaseError:
            logger.error("Unable to prepare for update: database error")
            self.retryUpdateTime = time.time() + aHandle.databaseRetryInterval
            raise
        except UpdateVSiteError:
            logger.error("Unable to prepare for update: agent error")
            self.retryUpdateTime = time.time() + self.updateRetryInterval
            raise

        # Do the updates.  We must be sure to finish_update on failure.
        try:
            self.update_groups(cur, end)
            self.update_users(cur, end)
        except DBDatabaseError:
            logger.error("Error while updating: database error")
            self.finish_update(cur, completed=False, end=end)
            self.retryUpdateTime = time.time() + aHandle.databaseRetryInterval
            raise
        except UpdateVSiteError:
            self.finish_update(cur, completed=False, end=end)
            logger.error("Error while updating: agent error")
            self.retryUpdateTime = time.time() + self.updateRetryInterval
            raise

        # If we can finish_update, we are done.  Otherwise reschedule.
        try:
            self.finish_update(cur, completed=True, end=end)
        except DBDatabaseError:
            logger.error("Unable to finish update: database error")
            self.retryUpdateTime = time.time() + aHandle.databaseRetryInterval
            raise
        except UpdateVSiteError:
            logger.error("Unable to finish update: agent error")
            self.retryUpdateTime = time.time() + self.updateRetryInterval
            raise

    def enable_bootstrap(self):
        """Begin the bootstrap process for this virtual site."""
        self.is_bootstrapping = True

    def get_timestamp(self):
        """Get the timestamp of last update."""
        aHandle = self.aHandle
        if self.is_bootstrapping:
            return aHandle.db_timestamp(1900,1,1, 0,0,0)
        return aHandle.get_timestamp(self.vsName)

    def set_timestamp(self, cur, ts=None):
        """Set the timestamp of the current update."""
        self.aHandle.set_timestamp(cur, self.vsName, ts)
        self.is_bootstrapping = False

    def run_escape(self, filtPath, op, u):
        """Apply a filter."""
        logger = self.aHandle.logger
        
        # Environment command
        envCmd = self.aHandle.envCmd
        
        cmd = [ envCmd ]
        for n in u.keys():
            cmd.append( 'HPCMAN_%s=%s' % (n, u[n]) )
        cmd += filtPath.split()
        cmd.append(op)
        try:
            rc = subprocess.call( cmd )
        except:
            logger.exception("Failed running filter %s", filtPath)
            rc = -1
        return rc



class SiteAgent(Agent):
    """Model an agent that can manage a site."""

    def __init__(self, agentKey, VSiteFactory):
        Agent.__init__(self, agentKey)
        self.is_bootstrapping = False


        # Set up slave listening.
        if self.slaveListenerPort is None:
            self.slaveAgentSet = None
        else:
            self.slaveAgentSet = SlaveAgentSet(self.slaveListenerPort)

        # Prepare to get virtual sites.
        self.VSiteFactory = VSiteFactory
        self.vSites = None

        # Some additional database attributes.
        self.conn_reset_time = 0
        self.isolation_level = None


    # -- options --

    def define_options(self):
        """Define configuration options for update agents."""
        Agent.define_options(self)
        parser = self.parser
        parser.add_option("-b", "--bootstrap",
                          action="store_true", dest="bootstrapFlag",
                          default=False,
                          help="bootstrap this agent")

    # -- get configuration data --

    def get_config_data(self):
        """Get configuration information."""
        Agent.get_config_data(self)
        self.get_config_data_sitename()
        self.get_config_data_slaves()
        self.get_config_data_masters()
        self.get_config_data_update_times()
        self.get_config_data_general_escapes()

    def get_config_data_sitename(self):
        """Site agents require a specific site name."""
        cp = self.configParser
        try:
            self.sitename = cp.get(self.CONSECT, "sitename")
        except:
            logging.fatal("Missing sitename")
            sys.exit(1)

    def get_config_data_slaves(self):
        """Address to listen for slaves needing to learn of updates."""
        cp = self.configParser
        try:
            a = cp.get(self.CONSECT, "slave_listener_port")
        except:
            self.slaveListenerPort = None
        else:
            self.slaveListenerPort = self._connection_split(a, "slave_listener_port", False)

    def get_config_data_masters(self):
        """Get configuration information to find master agents."""
        cp = self.configParser
        try:
            mrt = cp.get(self.CONSECT, "master_retry_interval")
        except:
            self.masterRetryInterval = 60.0
        else:
            try:
                self.masterRetryInterval = float(mrt)
            except:
                logging.fatal("Invalid value for 'master_retry_interval': %s",
                              mrt)
                sys.exit(1)
        try:
            mp = cp.get(self.CONSECT, "master_agent")
        except:
            self.masterAgentSet = None
        else:
            mas = self.masterAgentSet = MasterAgentSet()
            ma = self._connection_split(mp, "master_agent", True)
            mas.insert_connection(MasterAgentConnection(ma, self))


    def get_config_data_update_times(self):
        """Get some intervals for controlling updates."""
        # Retry interval for reconnecting to the database.
        cp = self.configParser
        try:
            dbrt = cp.get(self.CONSECT, "database_retry_interval")
        except:
            self.databaseRetryInterval = 60.0
        else:
            try:
                self.databaseRetryInterval = float(dbrt)
            except:
                logging.fatal("Invalid value for 'databaseRetryInterval': %s", dbrt)
                sys.exit(1)

        # Get the maximum nap time
        try:
            mnap = cp.get(self.CONSECT, "maximum_nap_time")
        except:
            self.maximum_nap_time = 36000.0
        else:
            try:
                self.maximum_nap_time = float(mnap)
            except:
                logging.float("Invalid value for 'maximum_nap_time': %s", mnap)
                sys.exit(1)

        # Get the update heartbeat.
        try:
            uhb = cp.get(self.CONSECT, "update_heartbeat")
        except:
            self.update_heartbeat = 3600.0
        else:
            try:
                self.update_heartbeat = float(uhb)
            except:
                logging.fatal("Invalid value for 'update_heartbeat': %s", uhb)
                sys.exit(1)


    def get_config_data_general_escapes(self):
        """Get general information for escapes."""
        # The location of the env command
        cp = self.configParser
        try:
            envCmd = cp.get(self.CONSECT, "env_command")
        except:
            self.envCmd = "/usr/bin/env"
        else:
            self.envCmd = envCmd
        # Get vSite update readiness escape
        try:
            vsite_ready_escape = cp.get(self.CONSECT, "vsite_ready_escape")
        except:
            self.vsite_ready_escape = None
        else:
            self.vsite_ready_escape = vsite_ready_escape

    # -- other...

    def get_vsites(self, cur):
        """Get all the virtual sites"""

        try:
            cur.execute("""SELECT vsName
                               FROM virtual_sites_allowed
                               WHERE siteName=%s""",
                        (self.sitename,))
            R = cur.fetchall()
        except DBDatabaseError:
            self.logger.info("Unable to get VSite list from database.")
            raise
        vSites = []
        vHandles = {}
        for res in R:
            vSite = res[0]
            vSites.append(vSite)
            vHandles[vSite] = self.VSiteFactory(self, vSite)
        self.vSites = vSites
        self.vHandles = vHandles
        self.logger.info("VSites served: %s", str(vSites)[1:-1])


    def enable_bootstrap(self, force=False):
        """Enable bootstrapping the agent."""

        self.is_bootstrapping = False
        if self.options.bootstrapFlag or force:
            self.is_bootstrapping = True


    # -- timestamps --

    def get_timestamp_filename(self, subkey=None):
        """Override this to include sitename in stamp."""
        fn = self.agentKey + "-" + self.sitename
        if subkey is not None:
            fn = fn + "-" + subkey
        return os.path.join(self.stateDir, fn+".timestamp")

    # -- database --

    def get_connection(self):
        """Get a database connection, setting some attributes."""
        conn = Agent.get_connection(self)
        self.conn_time = time.time()
        self.conn_reset_time = self.conn_time + self.update_heartbeat
        self.isolation_level = conn.isolation_level
        return conn


    def close_connection(self, **kw):
        """Close a database connection."""
        Agent.close_connection(self, **kw)
        now = time.time()
        self.conn_reset_time = now
        reconnectInterval = kw.get('reconnectInterval',
                                   self.databaseRetryInterval)
        self.reconnectDBTime = now + reconnectInterval
        self.isolation_level = None


    # -- site and vsites --
    
    def site_update(self, updateTime=None, endDict=None):
        """Update all the configured vsites."""
        for vh in self.vHandles.values():
            if self.is_bootstrapping:
                vh.enable_bootstrap()
        self.is_bootstrapping = False
        cur = self.get_cursor()
        try:
            for vsName, vh in self.vHandles.items():
                if updateTime is None or \
                   ( vh.retryUpdateTime is not None and \
                     vh.retryUpdateTime <= updateTime ):
                    try:
                        endTime = None
                        if endDict is not None and endDict.has_key(vsName):
                            endTime = endDict[vsName]
                        if self.vsite_ready_escape == None:
                            vh.vsite_update(cur, endTime)
                        else:
                            u = { 'vsite': vsName }
                            rc = vh.run_escape(self.vsite_ready_escape,
                                               'vsite_ready', u)
                            if rc == 0:
                                vh.vsite_update(cur, endTime)
                            else:
                                self.logger.warn('VSite %s is not ready', vsName)
                    except UpdateVSiteError:
                        self.logger.exception("Agent VSite update error.")
        finally:
            cur.close()


    # -- service interface --

    def main_loop(self):
        """Event loop: Handle connections.  Kick off updates."""

        logger = self.logger
        logger.debug("Entering main loop.")

        while True:

            r = []
            w = []
            cur = None

            # Get the time.  We'll compare to see if we need to
            # attempt (re)connections.
            now = time.time()

            # If it is time for a heartbeat, close the connection.  This
            # will force the restart and update.
            if self.conn is not None and \
                   self.conn_reset_time <= now+0.5:
                logger.debug("Time to reset database connection.")
                self.close_connection(reconnectInterval=0)

            # Check for the database connection.  If not present, see
            # if it is time to create it.  If we do create it, then
            # make sure we have the list of vsites we serve.  Finally,
            # see if we can perform updates.
            if self.conn is None and self.reconnectDBTime <= now+0.5:
                try:
                    cur = self.get_cursor()
                    if self.vSites is None:
                        self.get_vsites(cur)
                    if  self.masterAgentSet is None:
                        assert self.vSites is not None
                        self.site_update()
                except DBDatabaseError:
                    # Database connection broken.  Wait.
                    self.close_connection(cur=cur)
                    cur = None
                    logger.exception("Database connection broken; retry in %ds",
                                     self.databaseRetryInterval)
                except UpdateVSiteError:
                    logger.exception("Agent VSite update error.")
                    pass

            # See if any vSites are scheduled to be updated now.
            if self.conn is not None and self.vSites is not None:
                try:
                    self.site_update(now)
                except DBDatabaseError:
                    self.close_connection(cur=cur)
                    cur = None
                    logger.warn("Database connection broken; retry in %ds",
                                self.databaseRetryInterval)

            # Watch the database connection.  If there is no master,
            # then listen for events from the database (if connection
            # is present).
            if self.conn is not None and self.masterAgentSet is None:
                assert self.vSites is not None
                try:
                    self.conn.set_isolation_level(0)
                    cur = self.conn.cursor()
                    assert self.masterAgentSet is None
                    cur.execute("LISTEN hpcman_site")
                except:
                    # Connection dropped.
                    self.close_connection(cur=cur)
                    cur = None
                    logger.warn("Database connection broken; retry in %ds",
                                self.databaseRetryInterval)
                else:
                    if hasattr(cur, 'fileno'):
                        r.append(cur)
                    else:
                        r.append(self.conn)
		    #print 'Append: ', self.conn.fileno(), self.conn

            # If there should be a master, see if the connection is
            # present.  If not, check if it is time to connect.  If we
            # succeed, then send the site name and a list of vSites;
            # it is OK to block.  Switch to non-blocking for future
            # reads.
            if self.masterAgentSet is not None and \
               self.vSites is not None:
                self.masterAgentSet.work_if_ready(now, self)
                self.masterAgentSet.prepare_select(r, w)

            # Prepare for listening to connections to slaves, now that
            # any notifies have been queued up.
            if self.slaveAgentSet is not None:
                self.slaveAgentSet.prepare_select(r,w)

            # Compute timeout.  We have a maximum wait.
            timeCheck = now + self.maximum_nap_time
            logger.debug("timeout: now: %d", now)
            logger.debug("timeout: nap time ends: %d", timeCheck)
            if self.conn is None:
                timeCheck = min(timeCheck, self.reconnectDBTime)
                logger.debug("timeout: db reconnect: %d", timeCheck)
            if self.conn is not None:
                timeCheck = min(timeCheck, self.conn_reset_time)
                logger.debug("timeout: db reset: %d", timeCheck)
            if self.masterAgentSet is not None and \
               self.vSites is not None:
                timeCheck = self.masterAgentSet.compute_wake_time(timeCheck)
                logger.debug("timeout: master reconnect: %d", timeCheck)
            if self.vSites is not None:
                for vh in self.vHandles.values():
                    if vh.retryUpdateTime is not None:
                        timeCheck = min(timeCheck, vh.retryUpdateTime)
            if timeCheck < now:
                timeout = 0.0
            else:
                timeout = float(timeCheck - now)

            # Select.
            logger.debug("Select; timeout = %g, readers=%s, writers=%s",
                         timeout, repr(r), repr(w))
	    #print r,w,timeout
            r,w,x = select.select(r,w,[],timeout)

            # If we waited on a connection, restore isolation level.
            if self.isolation_level is not None:
                self.conn.set_isolation_level(self.isolation_level)

            # Handle slave connections.
            if self.slaveAgentSet is not None:
                self.slaveAgentSet.read_if_ready(r, self)
                self.slaveAgentSet.write_if_ready(w, self)

            # Handle timestamps from master.
            if self.masterAgentSet is not None:
                self.masterAgentSet.read_if_ready(r, self)
                self.masterAgentSet.write_if_ready(w, self)

            # Handle notifies from the database.
            if cur is not None and cur in r:
                try:
                    isReadyFlag = cur.isready()
                except DBDatabaseError:
                    # Apparently the connection died.
                    self.close_connection(cur=cur)
                    cur = None
                    logger.info("Database connection died; restart at %d",
                                self.reconnectDBTime)
                else:
                    try:
                        if isReadyFlag:
                            for n in self.conn.notifies:
                                if n[1] == "hpcman_site":
                                    logger.debug("Notified by DB of updates.")
                                    self.site_update()
                        else:
                            self.close_connection(cur=cur)
                            cur = None
                            #FIXME: message
                    except DBDatabaseError:
                        # Connection died.
                        self.close_connection(cur=cur)
                        cur = None
                        logger.exception("Database connection died; restart at %d",
                                         self.reconnectDBTime)

            # FIXME:
#            try:
#                for x in []: pass
#            except DBDatabaseError:
#                logger.exception("Weird exception encountered.")


    def notify_vsites(self, vsName):
        """Notify all slave vsites that a vsite was updated."""

        if self.slaveAgentSet is not None:
            #ts = self.vHandles[vsName].get_timestamp()
            self.slaveAgentSet.notify_vsite(vsName, self)


    def _connection_split(self, addr, cfName, needHost=False):
        """Split an address into components suitable for socket methods."""
        a2 = addr.split(":")
        if len(a2) == 1:
            if needHost:
                logging.fatal("Need host name in '%s': %s", cfName, addr)
                sys.exit(1)
            h = ""
            p = a2[0]
        elif len(a2) == 2:
            h,p = a2
        else:
            logging.fatal("Invalid format for '%s': %s", cfName, addr)
            sys.exit(1)
        try:
            p = int(p)
        except:
            logging.fatal("Invalid port for '%s': %s", cfName, addr)
            sys.exit(1)
        return (h,p)
