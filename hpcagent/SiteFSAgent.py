"""Site Agents that handle file system information."""

from SiteAgent import *

__all__ = [
    "VSiteFSAgent", "SiteFSAgent",
    # From SiteAgent
    "VSiteError", "UpdateVSiteError",
    "SlaveError", "SlaveSiteError",
    "SlaveConnectionError", "SlaveProtocolError",
    "VSiteAgent", "SiteAgent",
    # From Agent
    "Agent", "AgentError", "AgentDatabaseError"
    ]

class VSiteFSAgent(VSiteAgent):

    def start_get_user_information(self, cur, start=None, end=None):

        q = """SELECT uid,UserName,
                      gid,groupName,
                      homeDirectory,quota,
                      userAccountState,
                      projid,projName,projGroupName,
                      lastActive,created
               FROM vs_user_accounts
               WHERE siteName=%(siteName)s
                     AND vsName=%(vsName)s"""
        if start is not None:
            q += "      AND modified > %(start)s"
        if end is not None:
            q += "      AND modified <= %(end)s"
        q += " ORDER BY modified"
        cur.execute(q,
                    {"siteName": self.aHandle.sitename,
                     "vsName": self.vsName,
                     # "passwordType": self.aHandle.passwordType,
                     "start": start,
                     "end": end})

class SiteFSAgent(SiteAgent):
    pass
