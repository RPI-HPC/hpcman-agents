#! /usr/bin/env python

"""Proxy agent: This agent handles project, group, and user account
maintenance using proxy agents on remote systems.  Those proxies are
able to create groups, users, home directories, as well as modify
passwords, quotas, etc."""



import base64
import os, os.path
import subprocess
import shutil
import sys

from hpcagent.SiteProxyAgent import *
from hpcagent.DBHelpers import *


def IsCommandExecutable(cmd):
    """Test if a command seems to be executable, by testing first component."""
    cl = cmd.split()
    return os.path.isfile(cl[0]) and os.access(cl[0], os.X_OK)

class ProxyVSite(VSiteProxyAgent):
    """Proxy VSite agent."""

    def __init__(self, aHandle, vsName):

        VSiteProxyAgent.__init__(self, aHandle, vsName)

        cp = aHandle.configParser
        logger = aHandle.logger

        # Get configuration options we need for proxy processing.
        # FIXME:
        # -- Additional values for project
        # -- Additional values for user

        try:
            projectFilterEscape = cp.get(vsName, "project_filter")
        except:
            self.projectFilterEscape = None
        else:
            if IsCommandExecutable(projectFilterEscape):
                self.projectFilterEscape = projectFilterEscape
            else:
                logger.fatal("Project filter can not be executed: %s", projectFilterEscape)
                sys.exit(1)

        try:
            userFilterEscape = cp.get(vsName, "user_filter")
        except:
            self.userFilterEscape = None
        else:
            if IsCommandExecutable(userFilterEscape):
                self.userFilterEscape = userFilterEscape
            else:
                logger.fatal("User filter can not be executed: %s", userFilterEscape)
                sys.exit(1)

        try:
            proxyUser = cp.get(vsName, "proxyuser")
        except:
            self.proxyUserPairList = []
        else:
            pul = proxyUser.split(',')
            self.proxyUserPairList = pupl = []
            for huPair in pul:
                hu = huPair.split(':')
                if len(hu) == 1:
                    pupl.append( ( "*", hu[0].strip()))
                elif len(hu) == 2:
                    pupl.append( (hu[0].strip(), hu[1].strip()))
                else:
                    logger.fatal("Can not understand host,user pair: %s", huPair)
                    sys.exit(1)
        self.proxyUserPairList += [ ("*", None) ]

        try:
            proxyKey = cp.get(vsName, "proxykey")
        except:
            self.proxyKeyPairList = []
        else:
            pkl = proxyKey.split(',')
            self.proxyKeyPairList = pkpl = []
            for hkPair in pkl:
                hkl = hkPair.split(':')
                if len(hkl) == 1:
                    pkpl.append( ("*", hkl[0].strip()) )
                elif len(hkl) == 2:
                    pkpl.append( (hkl[0].strip(), hkl[1].strip()) )
                else:
                    logger.fatal("Can not understand host,key pair: %s", hkPair)
                    sys.exit(1)
        self.proxyKeyPairList +=  [ ("*", None) ]

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
                rc = self.run_escape(projectFilterEscape, 'project_filter', u)
                if rc != 0:
                    logger.debug("Project '%s' was filtered out, rc=%d", u['projName'], rc)
                    continue

            # See if we need to filter out this user, based on user.
            if userFilterEscape is not None:
                logger.debug("Check if user '%s' is filtered by %s",
                             u['UserName'], userFilterEscape)
                rc = self.run_escape(userFilterEscape, 'user_filter', u)
                if rc != 0:
                    logger.debug("User '%s' was filtered out, rc=%d", u['UserName'], rc)
                    continue

            # Process the proxy agents.
            self.run_agents('provision_users', u)
            self.run_agents_sending_password('password', u)


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

        cmd = self.build_proxy_command_chain(h, c, op, u)

        # Run the command.
        try:
            rc = subprocess.call( cmd )
        except:
            logger.exception('Failed running agent %s', c)
            rc = -1
        return rc


    def run_agents_sending_password(self, op, u):
        """Run all the agents, with an operation."""
        logger = self.aHandle.logger
        proxyAgentList = self.proxyAgentList
        for h,c in proxyAgentList:
            rc = self.run_agent_sending_password(h, c, op, u)
            if rc != 0:
                return rc

        return 0


    def run_agent_sending_password(self, h, c, op, u):
        """Run an agent on the remote host.  This is sent the hashed
        password."""
        logger = self.aHandle.logger

        cmd = self.build_proxy_command_chain(h, c, op, u)

        pw = u['password']
        if pw is None:
            # FIXME: Really want no password allowed.
            return 0
        pwh = base64.b64decode(pw)

        # Run the command.
        try:
            p = subprocess.Popen(cmd, stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                 close_fds=True)
            (po,pe) = p.communicate(pwh+'\n')
            print po,pe
            # FIXME: Handle output
            # FIXME: Make sure not None
            rc = p.returncode
        except:
            logger.exception('Failed running agent %s', c)
            rc = -1
        return rc


    def build_proxy_command_chain(self, h, c, op, u):
        """Build a command to run the proxy."""

        from fnmatch import fnmatch
        
        logger = self.aHandle.logger

        # FIXME: Allow these to change
        envCmd = '/usr/bin/env'
        sshCmd = '/usr/bin/ssh'

        # Build up a command to run.
        if h is not None:
            # Find user to run as.
            for hp,userAs in self.proxyUserPairList:
                if fnmatch(h, hp):
                    break
            # Find key to ssh with.
            for hp,keyAs in self.proxyKeyPairList:
                if fnmatch(h, hp):
                    break
            # Log.
            if userAs is None:
                extMsg = ""
            else:
                extMsg = " as " + userAs
            if keyAs is not None:
                extMsg += " using key " + keyAs
            logger.debug('Run remote proxy agent "%s %s" on %s%s for %s', c, op, h,
                         extMsg, u['username'])
            # Build the command.
            cmd = [ sshCmd ]
            if userAs is not None:
                cmd += [ '-l', userAs ]
            if keyAs is not None:
                cmd += [ '-i', keyAs ]
            cmd += [ '-tt', '-q', h, envCmd ]
            for n in u.keys():
                if n != 'password':
                    v = str(u[n])
                    v = v.replace("'", "'\\''")
                    cmd.append( "HPCMAN_%s='%s'" % ( n, v ) )
        else:
            logger.debug('Run local proxy agent "%s %s"', c, op)
            cmd = [ envCmd ]
            for n in u.keys():
                if n != 'password':
                    cmd.append( 'HPCMAN_%s=%s' % (n, u[n]) )
        cmd.append( c )
        cmd.append( op )
        return cmd

class ProxyAgent(SiteProxyAgent):

    def __init__(self, agentKey, VSiteFactory):
        SiteProxyAgent.__init__(self, agentKey, VSiteFactory)

        if self.passwordType not in ("sha256", "sha512"):
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
