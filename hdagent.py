#! /usr/bin/env python

import os, os.path
import subprocess
import shutil
import sys

# from hpcagent.SiteFSAgent import SiteFSAgent, VSiteFSAgent
from hpcagent.SiteFSAgent import *
from hpcagent.DBHelpers import *

def copytree(src, dst, uid, gid):
    """Copy a tree of files, setting uid and gid."""
    fl = os.listdir(src)
    os.mkdir(dst)
    shutil.copymode(src, dst)
    os.lchown(dst, uid, gid)
    for f in fl:
        srcF = os.path.join(src, f)
        dstF = os.path.join(dst, f)
        if os.path.isdir(srcF):
            copytree(srcF, dstF, uid, gid)
        else:
            shutil.copyfile(srcF, dstF)
            shutil.copymode(srcF, dstF)
            os.lchown(dstF, uid, gid)

class HomeDirVSite(VSiteFSAgent):
    """Home directory VSite Agent."""

    def __init__(self, aHandle, vsName):

        VSiteFSAgent.__init__(self, aHandle, vsName)

        cp = aHandle.configParser
        logger = aHandle.logger
        try:
            self.homeskeleton = cp.get(vsName, "homeskeleton")
        except:
            logger.fatal("Missing path to skeleton files")
            sys.exit(1)
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
        try:
            projectFilter = cp.get(vsName, "project_filter")
        except:
            # No project filter
            self.projectFilter = None
        else:
            if os.path.isfile(projectFilter):
                self.projectFilter = projectFilter
            else:
                logger.fatal("Project filter does not exist: %s",
                             projectFilter)
                sys.exit(1)
        try:
            projectEscape = cp.get(vsName, "project_escape")
        except:
            # No project escape
            self.projectEscape = None
        else:
            if os.path.isfile(projectEscape):
                self.projectEscape = projectEscape
            else:
                logger.fatal("Project escape does not exist: %s",
                             projectEscape)
                sys.exit(1)
        try:
            homeFilter = cp.get(vsName, "home_filter")
        except:
            # No home filter
            self.homeFilter = None
        else:
            if os.path.isfile(homeFilter):
                self.homeFilter = homeFilter
            else:
                logger.fatal("Home filter does not exist: %s",
                             homeFilter)
                sys.exit(1)
        try:
            homeEscape = cp.get(vsName, "home_escape")
        except:
            # No home escape
            self.homeEscape = None
        else:
            if os.path.isfile(homeEscape):
                self.homeEscape = homeEscape
            else:
                logger.fatal("Home escape does not exist: %s",
                             homeEscape)
                sys.exit(1)
        try:
            projectMaintEscape = cp.get(vsName, "project_maintenance_escape")
        except:
            # No project maintenance escape
            self.projectMaintEscape = None
        else:
            if os.path.isfile(projectMaintEscape):
                self.projectMaintEscape = projectMaintEscape
            else:
                logger.fatal("Project maintenance escape does not exist: %s",
                             projectMaintEscape)
                sys.exit(1)
        try:
            homeMaintEscape = cp.get(vsName, "home_maintenance_escape")
        except:
            # No home maintenance escape
            self.homeMaintEscape = None
        else:
            if os.path.isfile(homeMaintEscape):
                self.homeMaintEscape = homeMaintEscape
            else:
                logger.fatal("Home maintenance escape does not exist: %s",
                             homeMaintEscape)
                sys.exit(1)


    def update_users(self, cur, end=None):
        """Update users, e.g., create home directories."""
        vsName = self.vsName
        ts = self.timeStamp
        homeskeleton = self.homeskeleton
        projectFilter = self.projectFilter
        projectEscape = self.projectEscape
        homeFilter = self.homeFilter
        homeEscape = self.homeEscape
        projectMaintEscape = self.projectMaintEscape
        homeMaintEscape = self.homeMaintEscape
        aHandle = self.aHandle
        cp = aHandle.configParser
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

            #userAccountState = u[6]
            userAccountState = u['userAccountState']
            if userAccountState != "A":
                logger.info("Skip account %s, state %s", u['userName'], userAccountState)
                continue

            # +FIXME: Refactor?
            # -FIXME
            if self.homeformatFlag:
                hpath = cp.get(vsName, 'homeformat', False,
                               {'user': u['userName'],
                                'uid': u['uid'],
                                'group': u['groupName'],
                                'gid': u['gid'],
                                "homedir": u['homeDirectory'],
                                "project": u['projName'],
                                "projectid": u['projid'],
                                "projectgroup": u['projGroupName']})
            else:
                hpath = u['homeDirectory']
            u['homepath'] = hpath

            # Does this exist?
            if os.path.isdir(hpath):
                logger.debug("home directory already exists for user '%s'", u['userName'])
            elif os.path.exists(hpath):
                logger.warning("home directory '%s' for user '%s' exists and is not a directory",
                               hpath, u['userName'])
            else:
                logger.debug("Provision home directory for '%s'", u['userName'])
                if projectFilter is not None:
                    logger.debug("Check if project '%s' filtered by %s",
                                 u['projName'], projectFilter)
                    rc = self.run_escape(projectFilter, 'project_filter', u)
                    if rc != 0:
                        logger.debug("Project '%s' filtered out (%d)", u['projName'], rc)
                        continue
                if projectEscape is not None:
                    logger.debug("Check provisioning for project '%s' with %s",
                                 u['projName'], projectEscape)
                    rc = self.run_escape(projectEscape, 'project_escape', u)
                    if rc != 0:
                        logger.debug("Project '%s' provisioning failed (%d)",
                                     u['projName'], rc)
                        continue
                    logger.debug("Project is provisioned.")
                if homeFilter is not None:
                    logger.debug("Check if user '%s' filtered by %s",
                                 u['userName'], homeFilter)
                    rc = self.run_escape(homeFilter, 'home_filter', u)
                    if rc != 0:
                        logger.debug("User '%s' filtered out (%d)",
                                     u['userName'], rc)
                        continue
                try:
                    logger.debug("Creating user '%s' home directory",
                                 u['userName'])
                    copytree(homeskeleton, hpath, u['uid'], u['gid'])
                except:
                    logger.exception("Failed creating home directory.")
                else:
                    if homeEscape is not None:
                        logger.debug("Check provisioning for home directory '%s' with %s",
                                     u['userName'], homeEscape)
                        rc = self.run_escape(homeEscape, 'home_escape', u)
                        if rc != 0:
                            logger.debug("User '%s' provisioning failed (%d)",
                                         u['userName'], rc)
                            continue

            # Maintenance escapes, if home directory exists.
            if os.path.isdir(hpath) and projectMaintEscape is not None:
                logger.debug("Maintain provisioning for project '%s' with %s",
                             u['projName'], projectMaintEscape)
                rc = self.run_escape(projectMaintEscape, 'project_maint_escape', u)
                if rc == 0:
                    logger.debug("Project provisioning has been maintained.")
                else:
                    logger.error("Failed running project maintainance escape")
            if os.path.isdir(hpath) and homeMaintEscape is not None:
                logger.debug("Maintain provisioning for home directory '%s' with %s",
                             u['userName'], homeMaintEscape)
                rc = self.run_escape(homeMaintEscape, 'home_maint_escape', u)
                if rc != 0:
                    logger.debug("User '%s' provisioning failed (%d)",
                                 u['userName'], rc)
                    continue


class HomeDirUpdateAgent(SiteFSAgent):
    """Home directory agent for a site."""

    def define_options(self):
        """Define additional command line options for home directory tool."""
        SiteFSAgent.define_options(self)


def main():

    # Connect to database, and get configuration.
    aHandle = HomeDirUpdateAgent('hpchomedir', HomeDirVSite)

    # If bootstrapping, handle now.
    aHandle.enable_bootstrap()

    #aHandle.notification_loop()
    aHandle.main_loop()


if __name__ == "__main__":
    main()
