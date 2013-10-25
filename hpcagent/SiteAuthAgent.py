"""Site Agents that handle authentication information."""

import logging, sys

from SiteAgent import *
from DBHelpers import *

__all__ = [
    "VSiteAuthAgent", "SiteAuthAgent",
    # From SiteAgent
    "VSiteError", "UpdateVSiteError",
    "SlaveError", "SlaveSiteError",
    "SlaveConnectionError", "SlaveProtocolError",
    "VSiteAgent", "SiteAgent",
    # From Agent
    "Agent", "AgentError", "AgentDatabaseError"
    ]



class VSiteAuthAgent(VSiteAgent):

    def start_get_user_information(self, cur, start=None, end=None):

        q = """SELECT uid,UserName,
                      gid,groupName,
                      password,name,
                      shell,
                      homeDirectory,quota,
                      userAccountState,passwordMustChange,
                      modified,
                      projid,projName,projGroupName
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

    # FIXME: Deprecated.
    def get_user_information(self, cur, start=None, end=None):

        try:
            self.start_get_user_information(cur, start, end)
            return cur.fetchall()
        except DBDatabaseError:
            logger = self.aHandle.logger
            logger.debug("Database failure in 'get_user_information'")
            raise


    def get_group_information(self, cur, start=None, end=None):

        try:
            self.start_get_group_information(cur, start, end)
            return cur.fetchall()
        except DBDatabaseError:
            logger = self.aHandle.logger
            logger.debug("Database failure in 'get_group_information'")
            raise

    def get_group_members(self, cur, groupName, start=None, end=None):

        q = """SELECT userName
               FROM vs_group_members
               WHERE siteName=%(siteName)s
                     AND vsName=%(vsName)s
                     AND groupName=%(groupName)s"""
        if start is not None:
            q += """      AND modified > %(start)s"""
        if end is not None:
            q += """      AND modified <= %(end)s"""
        try:
            cur.execute(q, 
                        {"siteName": self.aHandle.sitename,
                         "vsName": self.vsName,
                         "groupName": groupName,
                         "start": start,
                         "end": end})
            return cur.fetchall()
        except DBDatabaseError:
            logger = self.aHandle.logger
            logger.debug("Database failure in 'get_group_members'")
            raise



class SiteAuthAgent(SiteAgent):

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
