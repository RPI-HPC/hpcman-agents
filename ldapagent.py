#! /usr/bin/env python

"""LDAP Agent"""

import sys
import ldap, ldap.modlist

from hpcagent.SiteAuthAgent import *
from hpcagent.DBHelpers import *

class LdapVSiteError(UpdateVSiteError): pass

def encodePassword(passwordType, password):
    if passwordType == 'ssha':
        return '{SSHA}'+password
    elif passwordType == 'md5':
        return '{MD5}'+password
    else:
        # FIXME: Programming error.
        return password

class LdapVSite(VSiteAuthAgent):

    def __init__(self, aHandle, vsName):

        VSiteAuthAgent.__init__(self, aHandle, vsName)
        
        cp = aHandle.configParser
        try:
            self.ldapURI = cp.get(vsName, "uri")
        except:
            aHandle.logger.fatal("Missing URI for LDAP server.")
            sys.exit(1)
        self.dn = cp.get(vsName, "dn")
        try:
            self.admin = cp.get(vsName, "admin")
        except:
            self.admin = 'cn=Manager,' + dn
        self.adminPass = cp.get(vsName, "password")
        try:
            self.userOU = cp.get(vsName, "userbase")
        except:
            self.userOU = 'ou=People,'+self.dn
        try:
            self.groupOU = cp.get(vsName, "groupbase")
        except:
            self.groupOU = 'ou=Groups,'+self.dn
        # +FIXME: Refactor?
        try:
            homeformat = cp.get(vsName, "homeformat", raw=True)
        except:
            # No homeformat.
            self.homeformatFlag = False
        else:
            try:
                hf = homeformat % { "user": "user",
                                    "uid": 0,
                                    "group": "group",
                                    "gid": 0,
                                    "homedir": "/homedir",
                                    "project": "PROJ",
                                    "projectid": 0,
                                    "projectgroup": "PROJECTGROUP"}
            except:
                logger.fatal("Invalid home directory format (homeformat)")
                sys.exit(1)
            self.homeformatFlag = True
        # -FIXME


    def _vsite_bootstrap(self):
        aHandle = self.aHandle
        vsName = self.vsName
        logger = aHandle.logger
        logger.info("Bootstrap LDAP for '%s'.", vsName)
        # FIXME: Add actual code to bootstrap.
        #        This must also force complete updates from dawn of time.

        # Create the top organizational units.  We need to figure out
        # the "dc" and "ou" components.
        dn = self.dn
        dnc = dn.split(",")
        if dnc[0][:3] != "dc=":
            logger.error("Cannot determine dc= component of '%s' for '%s'",
                         dn, vsName)
        else:
            l = self.connect_ldap()
            try:
                d = {
                    "dc": dnc[0][3:],
                    "description": "Root LDAP entry for %s" % (vsName,),
                    "objectClass": ["dcObject", "organizationalUnit", "top"],
                    "ou": "rootobject"
                    }
                l.add_s(dn, ldap.modlist.addModlist(d))
                logger.debug("Created '%s' for '%s'", dn, vsName)
            except:
                logger.exception("Failed to create '%s' for '%s'", dn, vsName)

        userOU = self.userOU
        userOUc = userOU.split(",")
        if userOUc[0][:3] != "ou=":
            logger.error("Cannot determine ou= component of '%s' for '%s'",
                         userOU, vsName)
        else:
            try:
                d = {
                    "ou": userOUc[0][3:],
                    "description": "People and virtual organizations",
                    "objectClass": "organizationalUnit"
                    }
                l.add_s(self.userOU, ldap.modlist.addModlist(d))
                logger.debug("Created '%s' for '%s'", userOU, vsName)
            except:
                logger.exception("Failed to create '%s' for '%s'",
                                 userOU, vsName)

        groupOU = self.groupOU
        groupOUc = groupOU.split(",")
        if groupOUc[0][:3] != "ou=":
            logger.error("Cannot determine ou= component of '%s' for '%s'",
                         groupOUc, vsName)
        else:
            try:
                d = {
                    "ou": groupOUc[0][3:],
                    "description": "Posix groups",
                    "objectClass": "organizationalUnit"
                    }
                l.add_s(self.groupOU, ldap.modlist.addModlist(d))
                logger.debug("Created '%s' for '%s'", groupOU, vsName)
            except:
                logger.exception("Failed to create '%s' for '%s'",
                                 groupOU, vsName)
            del l

    def connect_ldap(self):
        """Connect to the LDAP server."""

        self.aHandle.logger.debug("Connecting to LDAP server '%s'",
                                  self.ldapURI)
        try:
            l = ldap.initialize(self.ldapURI)
            l.bind_s(self.admin, self.adminPass)
        except ldap.LDAPError:
            raise LdapVSiteError,None,sys.exc_info()[2]
        return l

    def vsite_update(self, cur, end=None):
        """Update this VSite."""
        
        if self.is_bootstrapping:
            self._vsite_bootstrap()
        VSiteAuthAgent.vsite_update(self, cur, end)
        self.is_bootstrapping = False


    def prepare_update(self, cur):
        logger = self.aHandle.logger
        cur = VSiteAuthAgent.prepare_update(self, cur)
        try:
            self.ldapHandle = self.connect_ldap()
        except ldap.LDAPError,ei:
            logger.warning("Unable to connect to LDAP server '%s': %s",
                           self.ldapURI, ei.message)
            raise LdapVSiteError,None,sys.exc_info()[2]
        return cur


    def finish_update(self, cur, completed, end=None):
        del self.ldapHandle
        VSiteAuthAgent.finish_update(self, cur, completed, end)


    def update_groups(self, cur, end=None):
        """Update groups."""

        # FIXME: Handle deletions

        l = self.ldapHandle
        vsName = self.vsName
        ts = self.timeStamp
        groupOU = self.groupOU
        aHandle = self.aHandle
        logger = aHandle.logger
        logger.info("Updating groups.")

        # FIXME: Get a second cursor.
        # FIXME: Isolate database and ldap errors.
        for g in self.get_group_information(cur, start=ts, end=end):
            dn = 'cn=%s,%s' % (g[1], groupOU)
            d = {
                'objectClass': ['posixGroup'],
                'cn': [g[1]],
                'gidNumber': [str(g[0])]}
            d['memberUid'] = [m[0]
                              for m in
                              self.get_group_members(cur,g[1],start=ts,end=end)]
            try:
                (odn,od) = l.search_s(dn, ldap.SCOPE_BASE)[0]
            except ldap.NO_SUCH_OBJECT:
                logger.debug("Creating group '%s'",g[1])
                l.add_s(dn, ldap.modlist.addModlist(d))
            else:
                logger.debug("Modifying group '%s'",g[1])
                l.modify_s(dn, ldap.modlist.modifyModlist(od, d))

    def update_users(self, cur, end=None):
        """Update users."""

        l = self.ldapHandle
        vsName = self.vsName
        ts = self.timeStamp
        userOU = self.userOU
        aHandle = self.aHandle
        cp = aHandle.configParser
        logger = aHandle.logger
        passwordType = aHandle.passwordType
        logger.info("Updating users.")
        try:
            self.start_get_user_information(cur, start=ts, end=end)
        except DBDatabaseError:
            logger.info("Unable to query user information.")
            raise
        while True:
            try:
                u = DB_get_next_row(cur)
            except DBDatabaseError:
                logger.info("Unable to fetch user information")
                raise
            if u is None:
                break
            dn = 'uid=%s,%s' % (u[1], userOU)
            userAccountState = u[9]
            logger.debug("Process user '%s', state=%s, passwordMustChange=%s",
                         u[1], userAccountState, u[10])
            #FIXME:
            #    Make the /bin/false shell be configurable.
            try:
                if userAccountState == "D":
                    try:
                        (odn,od) = l.search_s(dn, ldap.SCOPE_BASE)[0]
                    except ldap.NO_SUCH_OBJECT:
                        pass
                    else:
                        d = {
                            'loginShell': '/bin/false'
                            }
                        if od.has_key('userPassword'):
                            d['userPassword'] = ''
                        logger.debug("Disabling user '%s'",u[1])
                        modList = ldap.modlist.modifyModlist(od, d, ignore_oldexistent=1)
                        logger.debug("Modlist: %s",repr(modList))
                        l.modify_s(dn, modList)
                        logger.debug("Disabling complete for '%s'",u[1])
                elif userAccountState == "R":
                    try:
                        l.delete_s(dn)
                        logger.debug("Deleted user '%s'", u[1])
                    except ldap.NO_SUCH_OBJECT:
                        pass
                else: # userAccountState == 'A'
                    # FIXME: Disable passwords if password must be reset.
                    # +FIXME: Refactor?
                    if self.homeformatFlag:
                        hpath = cp.get(vsName, 'homeformat', False,
                                       {'user': u[1],
                                        'uid': u[0],
                                        'group': u[3],
                                        'gid': u[2],
                                        "homedir": u[7],
                                        "project": u[13],
                                        "projectid": u[12],
                                        "projectgroup": u[14]})
                    else:
                        hpath = u[7]
                    logger.debug("Home path is '%s'", hpath)
                    # -FIXME
                    logger.debug("About to add user '%s'", u[1])
                    d = {
                        'objectClass': ['account', 'posixAccount'],
                        'uid': [u[1]],
                        'cn': [u[5]],
                        'gecos': [u[5]],
                        'uidNumber': [str(u[0])],
                        'gidNumber': [str(u[2])],
                        'loginShell': [u[6]],
                        'homeDirectory': [hpath]}
                    if u['passwordMustChange']:
                        d['loginShell'] = '/bin/false'
                    elif u[4] is not None:
                        d['userPassword'] = [encodePassword(passwordType, u[4])]
                    try:
                        (odn,od) = l.search_s(dn, ldap.SCOPE_BASE)[0]
                    except ldap.NO_SUCH_OBJECT:
                        logger.debug("Creating user '%s'",u[1])
                        l.add_s(dn, ldap.modlist.addModlist(d))
                        logger.debug("Creation complete for '%s'",u[1])
                    else:
                        logger.debug("Modifying user '%s'",u[1])
                        modList = ldap.modlist.modifyModlist(od, d)
                        logger.debug("Modlist: %s",repr(modList))
                        l.modify_s(dn, modList)
                        logger.debug("Modification complete for '%s'",u[1])
            except ldap.LDAPError:
                logger.exception("Unexpected LDAP error!")
                raise LdapVSiteError,None,sys.exc_info()[2]


class LdapUpdateAgent(SiteAuthAgent):

    def __init__(self, agentKey, VSiteFactory):
        SiteAuthAgent.__init__(self, agentKey, VSiteFactory)

        if self.passwordType not in ("ssha", "md5"):
            self.logger.fatal("Unknown password type '%s'",
                              self.passwordType)
            sys.exit(1)

    def define_options(self):
        """Define command line options for LDAP tool."""
        SiteAuthAgent.define_options(self)
        # Are there any specific LDAP configs?


def main():

    # Connect to database, and get configuration.
    aHandle = LdapUpdateAgent("hpcldap", LdapVSite)

    # If bootstrapping, handle now.
    aHandle.enable_bootstrap()

    #aHandle.notification_loop()
    aHandle.main_loop()


if __name__ == "__main__":
    main()
