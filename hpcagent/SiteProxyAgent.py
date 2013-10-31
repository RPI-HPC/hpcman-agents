"""Site agents that handle everything about users and projects.

This might eventually be able to replace SiteAuthAgent and SiteFSAgent."""

import logging, sys

from SiteAgent import *
from DBHelpers import *

__all__ = [
    "VSiteProxyAgent", "SiteProxyAgent",
    # From SiteAgent
    "VSiteError", "UpdateVSiteError",
    "SlaveError", "SlaveSiteError",
    "SlaveConnectionError", "SlaveProtocolError",
    "VSiteAgent", "SiteAgent",
    # From Agent
    "Agent", "AgentError", "AgentDatabaseError"
    ]



class VSiteProxyAgent(VSiteAgent):

    def start_get_user_information(self, cur, start=None, end=None):

        q = """SELECT uid,UserName,
                      gid,groupName,
                      password,name,
                      shell,
                      homeDirectory,quota,
                      userAccountState,passwordMustChange,
                      modified,
                      projid,projName,projGroupName,
                      lastActive,created
               FROM vs_user_accounts
               WHERE siteName=%(siteName)s
                     AND vsName=%(vsName)s
                     AND passwordType=%(passwordType)s"""
        if start is not None:
            q += "      AND modified > %(start)s"
        if end is not None:
            q += "      AND modified <= %(end)s"
        q += " ORDER BY modified"
        cur.execute(q,
                    {"siteName": self.aHandle.sitename,
                     "vsName": self.vsName,
                     "passwordType": self.aHandle.passwordType,
                     "start": start,
                     "end": end})

    def start_get_group_information(self, cur, start=None, end=None):

        q = """SELECT gid,groupName,groupState,modified
               FROM vs_groups
               WHERE siteName=%(siteName)s
                     AND vsName=%(vsName)s"""
        if start is not None:
            q += """      AND modified > %(start)s"""
        if end is not None:
            q += """      AND modified <= %(end)s"""
        q += " ORDER BY modified"
        cur.execute(q,
                    {"siteName": self.aHandle.sitename,
                     "vsName": self.vsName,
                     "start": start,
                     "end": end})




class SiteProxyAgent(SiteAgent):

    def __init__(self, agentKey, VSiteFactory):
        SiteAgent.__init__(self, agentKey, VSiteFactory)


    def get_config_data(self):
        """Get configuration information."""
        SiteAgent.get_config_data(self)
        cp = self.configParser

        # Authentication agents need to know what hash type is used.
        try:
            self.passwordType = cp.get(self.CONSECT, "password_type")
        except:
            logging.fatal("No password_type specified.")
            sys.exit(1)

