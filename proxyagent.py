#! /usr/bin/env python

"""Proxy agent: This agent handles project, group, and user account
maintenance using proxy agents on remote systems.  Those proxies are
able to create groups, users, home directories, as well as modify
passwords, quotas, etc."""



import os, os.path
import subprocess
import shutil
import sys

from hpcagent.SiteProxyAgent import *
from hpcagent.DBHelpers import *


class ProxyVSite(VSiteProxyAgent):
    """Proxy VSite agent."""

    def __init__(self, aHandle, vsName):

        VSiteProxyAgent.__init__(self, aHandle, vsName)

        cp = aHandle.configParser
        logger = aHandle.logger

        # Get configuration options we need for proxy processing.
        # FIXME:
        # -- hostname:path-to-helper,  ...
        # -- Additional values for project
        # -- Additional values for user

        try:
            projectFilterEscape = cp.get(vsName, "project_filter")
        except:
            self.projectFilterEscape = None
        else:
            if os.path.isfile(projectFilterEscape) and os.access(projectFilterEscape, os.X_OK):
                self.projectFilterEscape = projectFilterEscape
            else:
                logger.fatal("Project filter can not be executed: %s", projectFilterEscape)
                sys.exit(1)

        try:
            userFilterEscape = cp.get(vsName, "user_filter")
        except:
            self.userFilterEscape = None
        else:
            if os.path.isfile(userFilterEscape) and os.access(userFilterEscape, os.X_OK):
                self.userFilterEscape = userFilterEscape
            else:
                logger.fatal("User filter can not be executed: %s", userFilterEscape)
                sys.exit(1)

        try:
            proxyAgent = cp.get(vsName, "proxyagent")
        except:
            self.proxyAgentList = []
        else:
            proxyAgentList = []
            proxyPairs = proxyAgent.split(',')
            for p in proxyPairs:
                hc = p.split(':')
                if len(hc) == 1:
                    h = None
                    c = hc[0].strip()
                elif len(hc) == 2:
                    h = hc[0].strip()
                    c = hc[1].strip()
                else:
                    log.fatal("Can not understand host, proxy pair: %s", p)
                    sys.exit(1)
                # FIXME: Sanity check: does 'c' exist on 'h' ???
                proxyAgentList.append( (h,c) )
            self.proxyAgentList = proxyAgentList

    def vsite_update(self, cur, end=None):
        """Update this VSite."""
        
        VSiteProxyAgent.vsite_update(self, cur, end)
        self.is_bootstrapping = False

    def prepare_update(self, cur):
        logger = self.aHandle.logger
        ts = VSiteProxyAgent.prepare_update(self, cur)
        # FIXME: Anything more?


    def finish_update(self, cur, completed, end=None):
        #FIXME: Anything more?
        VSiteProxyAgent.finish_update(self, cur, completed, end)


    def update_groups(self, cur, end=None):
        """Update groups."""
        pass

    def update_users(self, cur, end=None):
        """Update users, doing whatever is needed for passwords, etc."""

        aHandle = self.aHandle
        vsName = self.vsName
        ts = self.timeStamp
        cp = aHandle.configParser

        projectFilterEscape = self.projectFilterEscape
        userFilterEscape = self.userFilterEscape
        proxyAgentList = self.proxyAgentList

        logger = aHandle.logger
        logger.info("Updating users.")

        # Get cursor to user information, filtered by timestamp.
        try:
            self.start_get_user_information(cur, start=ts, end=end)
        except DBDatabaseError:
            logger.info("Unable to query user information.")
            raise

        # Iterate over selected users.
        while True:
            try:
                u = DB_get_next_row(cur)
            except DBDatabaseError:
                logger.info("Unable to fetch user information")
                raise
            if u is None:
                break

            # See if we need to filter out this user, based on project.
            if projectFilterEscape is not None:
                logger.debug("Check if project '%s' is filtered by %s",
                             u['projName'], projectFilterEscape)
                rc = self.apply_filter(projectFilterEscape, u)
                if rc != 0:
                    logger.debug("Project '%s' was filtered out, rc=%d", u['projName'], rc)
                    continue

            # See if we need to filter out this user, based on user.
            if userFilterEscape is not None:
                logger.debug("Check if user '%s' is filtered by %s",
                             u['UserName'], userFilterEscape)
                rc = self.apply_filter(userFilterEscape, u)
                if rc != 0:
                    logger.debug("User '%s' was filtered out, rc=%d", u['UserName'], rc)
                    continue

            # Process the proxy agents.
            self.run_agents('provision_users', u)


    def apply_filter(self, filtPath, u):
        """Apply a filter."""
        logger = self.aHandle.logger
        
        # FIXME: Allow this to change.
        envCmd = '/usr/bin/env'
        
        cmd = [ envCmd ]
        for n in u.column_desc:
            cmd.append( 'HPCMAN_%s=%s' % (n, u[n]) )
        cmd.append( filtPath )
        try:
            rc = subprocess.call( cmd )
        except:
            logger.exception("Failed running filter %s", filtPath)
            rc = -1
        return rc


    def run_agents(self, op, u):
        """Run all the agents, with an operation."""
        logger = self.aHandle.logger
        proxyAgentList = self.proxyAgentList
        for h,c in proxyAgentList:
            rc = self.run_agent(h, c, op, u)
            if rc != 0:
                return rc

        return 0

    def run_agent(self, h, c, op, u):
        """Run an agent on the remote host."""
        logger = self.aHandle.logger

        # FIXME: Allow these to change
        envCmd = '/usr/bin/env'
        sshCmd = '/usr/bin/ssh'

        # Build up a command to run.
        if h is not None:
            logger.debug('Run remote proxy agent "%s %s" on %s', c, op, h)
            cmd = [ sshCmd, h, envCmd ]
            for n in u.column_desc:
                v = str(u[n])
                v = v.replace("'", "'\\''")
                cmd.append( "HPCMAN_%s='%s'" % ( n, v ) )
        else:
            logger.debug('Run local proxy agent "%s %s"', c, op)
            cmd = [ envCmd ]
            for n in u.column_desc:
                cmd.append( 'HPCMAN_%s=%s' % (n, u[n]) )
        cmd.append( c )
        cmd.append( op )

        # Run the command.
        try:
            rc = subprocess.call( cmd )
        except:
            logger.exception('Failed running agent %s', c)
            rc = -1
        return rc


class ProxyAgent(SiteProxyAgent):

    def __init__(self, agentKey, VSiteFactory):
        SiteProxyAgent.__init__(self, agentKey, VSiteFactory)

        if self.passwordType not in ("ssha", "md5"):
            self.logger.fatal("Unknown password type '%s'",
                              self.passwordType)
            sys.exit(1)

    def define_options(self):
        """Define command line options for LDAP tool."""
        SiteProxyAgent.define_options(self)
        # FIXME: Get the necessary proxy list
        



def main():
    # Connect to database, and get configuration.
    aHandle = ProxyAgent("hpcproxy", ProxyVSite)

    # If bootstrapping, handle now.
    aHandle.enable_bootstrap()

    #aHandle.notification_loop()
    aHandle.main_loop()


if __name__ == "__main__":
    main()
