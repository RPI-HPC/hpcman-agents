"""Model connections to masters."""

import socket, time, xdrlib
from ConnectSet import Connection, ConnectionSet

__all__ = [
    "MasterAgentConnection", "MasterAgentSet"
    ]

class MasterAgentConnection(Connection):
    """Model a connection to a master."""

    def __init__(self, addr, aHandle):
        Connection.__init__(self, addr, aHandle)
        self._connectTime = 0  # Dawn of time -- we're late!


    def handle_disconnect(self, aHandle):
        """Prepare for a fresh connection to the master."""

        try:
            if self._s is not None:
                self._s.close()
        except:
            pass
        self._s = None
        self._connectTime = time.time() + aHandle.masterRetryInterval
        self._rbuf = ""
        self._wbuf = ""
        aHandle.logger.warn("Connection to master failed; next retry at %d",
                    self._connectTime)


    def handle_input(self, aHandle):
        """Process input."""

        # FIXME: We should modify to store ENTIRE messages as one
        # string, then unpack pieces out of that.  Then we can
        # determine whether the message broken, or incomplete.

        while True:
            u = xdrlib.Unpacker(self._rbuf)
            try:
                vSite = u.unpack_string()
                ts = u.unpack_string()
            except:
                # FIXME: Presuming it is incomplete.
                return
            self._rbuf = self._rbuf[u.get_position():]

            self.process_input_data(aHandle, vSite, ts)


    def process_input_data(self, aHandle, vSite, ts):

        # It is a message to update a vsite to a certain time.
        logger = aHandle.logger
        logger.debug("Requested to update vsite %s to %s.",
                     vSite, ts)
        # FIXME: How?
        #
        # Probably want to schedule update ASAP, which will happen when
        # there is a database connection.  But somehow need to associate
        # this timestamp with the vsite.

        try:
            vh = aHandle.vHandles[vSite]
        except KeyError:
            logger.error("Update received for unrecognized vSite '%s'",
                         vSite)
            return
        cur = None
        try:
            cur = aHandle.get_cursor()
            vh.vsite_update(cur, ts)
            cur.close()
        except DBDatabaseError:
            # Note: All vSites are notified when the database
            # connection is restored.
            #
            aHandle.close_connection(cur=cur)
            logger.info("Database connection apparently died; restart at %d",
                        aHandle.reconnectDBTime)
        except UpdateVSiteError:
            logger.exception("Agent update error for vSite '%s'", vSite)
            try:
                cur.close()
            except:
                pass
        

    def work_if_ready(self, now, aHandle):
        """Connect to master if unconnected, and scheduled."""

        if self._s is None and self._connectTime <= now:
            logger = aHandle.logger
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect(self._addr)
            except:
                self.handle_disconnect(aHandle)
                return
            self._s = s
            p = xdrlib.Packer()
            p.pack_string(aHandle.sitename)
            p.pack_list(aHandle.vSites, p.pack_string)
            self._wbuf += p.get_buffer()
            logger.debug("Connection welcome queued to master.")


    def compute_wake_time(self, currentWakeTime):
        if self._s is None:
            print "Compting wake time"
            currentWakeTime = min(currentWakeTime, self._connectTime)
        return currentWakeTime
            
            
class MasterAgentSet(ConnectionSet):
    """Model connections to a set of masters."""

    def __init__(self):
        ConnectionSet.__init__(self)

    def compute_wake_time(self, currentWakeTime):
        for c in self._conns:
            currentWakeTime = c.compute_wake_time(currentWakeTime)
        return currentWakeTime


#    def add_master(self, hostAddr, port, aHandle):
#        """Add a connection to a master."""
#
#        c = MasterAgentConnection((hostAddr, port), aHandle)
#        self.insert_connection(c)


    def get_timestamp(self):
        """Get the min latest time of all masters."""

        
