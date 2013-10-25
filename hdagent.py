#! /usr/bin/env python

import os, os.path
import subprocess
import shutil
import sys

# from hpcagent.SiteFSAgent import SiteFSAgent, VSiteFSAgent
from hpcagent.SiteFSAgent import *

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
        for u in self.get_user_information(cur, ts):

            userAccountState = u[6]
            if userAccountState != "A":
                logger.info("Skip account %s, state %s", u[1], userAccountState)
                continue

            # +FIXME: Refactor?
            # -FIXME
            if self.homeformatFlag:
                hpath = cp.get(vsName, 'homeformat', False,
                               {'user': u[1],
                                'uid': u[0],
                                'group': u[3],
                                'gid': u[2],
                                "homedir": u[4],
                                "project": u[8],
                                "projectid": u[7],
                                "projectgroup": u[9]})
            else:
                hpath = u[4]

            # Does this exist?
            if os.path.isdir(hpath):
                logger.debug("home directory already exists for user '%s'", u[1])
            elif os.path.exists(hpath):
                logger.warning("home directory '%s' for user '%s' exists and is not a directory",
                               hpath, u[1])
            else:
                logger.debug("Provision home directory for '%s'", u[1])
                if projectFilter is not None:
                    try:
                        logger.debug("Check if project '%s' filtered by %s",
                                     u[8], projectFilter)
                        rc = subprocess.call([projectFilter,
                                              hpath,     # homeDirectory
                                              u[8],      # projName
                                              str(u[7]), # projID
                                              u[9],      # projGroupName
                                              str(u[2]), # gid
                                              str(u[10]), # user lastActive
                                              str(u[11])  # user created
                                              ])
                    except:
                        logger.exception("Failed running project filter")
                        rc = -1
                    if rc != 0:
                        logger.debug("Project '%s' filtered out (%d)", u[8], rc)
                        continue
                if projectEscape is not None:
                    try:
                        logger.debug("Check provisioning for project '%s' with %s",
                                     u[8], projectEscape)
                        rc = subprocess.call([projectEscape,
                                              hpath,     # Home directory
                                              u[8],      # Project name
                                              str(u[7]), # Project ID
                                              u[9],      # Project group name
                                              str(u[2])  # gid
                                              ])
                        logger.debug("Project is provisioned.")
                    except:
                        logger.exception("Failed running project escape")
                        rc = -1
                    if rc != 0:
                        logger.debug("Project '%s' provisioning failed (%d)",
                                     u[8], rc)
                        continue
                if homeFilter is not None:
                    try:
                        logger.debug("Check if user '%s' filtered by %s",
                                     u[2], homeFilter)
                        rc = subprocess.call([homeFilter,
                                              hpath,      # homeDirectory
                                              u[8],       # projName
                                              str(u[7]),  # projID
                                              u[9],       # projGroupName
                                              str(u[2]),  # gid
                                              str(u[10]), # user lastActive
                                              str(u[11])  # user created
                                              ])
                    except:
                        logger.exception("Failed running home filter")
                        rc = -1
                    if rc != 0:
                        logger.debug("User '%s' filtered out (%d)", u[2], rc)
                        continue
                try:
                    logger.debug("Creating user '%s' home directory", u[1])
                    copytree(homeskeleton, hpath, u[0], u[2])
                except:
                    logger.exception("Failed creating home directory.")
                else:
                    if homeEscape is not None:
                        try:
                            logger.debug("Check provisioning for home directory '%s' with %s",
                                         u[2], homeEscape)
                            rc = subprocess.call([homeEscape,
                                                  hpath,  # Home path
                                                  u[1],   # User name
                                                  u[3],   # Group name
                                                  u[8],   # projName,
                                                  u[9],   # projGroupName
                                                  str(u[0]), # uid
                                                  str(u[2]), # gid
                                                  str(u[7])  # projID
                                                  ])
                        except:
                            logger.exception("Failed running home escape.")
                            rc = -1
                        if rc != 0:
                            logger.debug("User '%s' provisioning failed (%d)",
                                         u[2], rc)
                            continue

            # Maintenance escapes, if home directory exists.
            if os.path.isdir(hpath) and projectMaintEscape is not None:
                try:
                    logger.debug("Maintain provisioning for project '%s' with %s",
                                 u[8], projectMaintEscape)
                    os.spawnl(os.P_WAIT, projectMaintEscape, projectMaintEscape,
                              u[4], u[8], str(u[7]), u[9], str(u[2]))
                    logger.debug("Project provisioning has been maintained.")
                except:
                    logger.exception("Failed running project maintainance escape")
            if os.path.isdir(hpath) and homeMaintEscape is not None:
                try:
                    logger.debug("Maintain provisioning for home directory '%s' with %s",
                                 u[1], homeMaintEscape)
                    rc = subprocess.call([homeMaintEscape,
                                          hpath,    # Home path
                                          u[1],     # User name
                                          u[3],     # Group name
                                          u[8],     # projName
                                          u[9],     # projGroupName
                                          str(u[0]), # uid
                                          str(u[2]), # gid
                                          str(u[7])  # projID
                                          ])
                except:
                    logger.exception("Failed running home maintenance escape.")
                    rc = -1
                if rc != 0:
                    logger.debug("User '%s' provisioning failed (%d)",
                                 u[2], rc)
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
