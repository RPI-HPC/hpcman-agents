""" Slave connections"""

import socket, xdrlib
from ConnectSet import Connection, ConnectionSet, ConnectionError

from Agent import AgentError

__all__ = [
    "SlaveAgentSet",
    "SlaveError", "SlaveSiteError",
    "SlaveConnectionError", "SlaveProtocolError",
    ]

class SlaveError(ConnectionError): pass

class SlaveSiteError(SlaveError): pass

class SlaveConnectionError(SlaveError): pass

class SlaveProtocolError(SlaveConnectionError): pass


def _testListSubset(candSub, candSuper):
    """Test if elements of 'candSub' are all elements of 'candSuper'."""

    for e in candSub:
        if e not in candSuper:
            return False
    return True



class SlaveAgentConnection(Connection):
    """Model a connection to an enslaved agent."""

    def __init__(self, s, addr, aHandle):
        Connection.__init__(self, addr, aHandle)
        self._s = s
        self._vSites = []
        aHandle.logger.debug("New slave connection from %s", repr(addr))


    def handle_disconnect(self, aHandle):
        """Disconnect a slave."""
        try:
            self._s.close()
        except:
            pass
        self._s = None
        raise SlaveConnectionError


    def handle_input(self, aHandle):
        """Handle input from slave."""
        while True:
            rbuf = self._rbuf
            try:
                u = xdrlib.Unpacker(rbuf)
                slaveSite = u.unpack_string()
                slaveVSites = u.unpack_list(u.unpack_string)
            except:
                # No, it is not complete.
                return
            self._rbuf = rbuf[u.get_position():]
            self.process_input_data(aHandle, slaveSite, slaveVSites)


    def process_input_data(self, aHandle, slaveSite, slaveVSites):

        logger = aHandle.logger
        s = self._s

        # Make sure we can serve this slave.
        if slaveSite != aHandle.sitename:
            logger.warning("Slave belongs to wrong site.")
            s.close()
            self._s = None
            raise SlaveSiteError
        if not _testListSubset(slaveVSites, aHandle.vSites):
            logger.warning("Slave has vSites not managed by this agent.")
            s.close()
            self._s = None
            raise SlaveSiteError

        # Store the list of vSites for which this slave needs information.
        self._vSites = slaveVSites

        # Send the timestamp to each VSite.
        for vsName in slaveVSites:
            ts = aHandle.vHandles[vsName].get_timestamp()
            self.notify_vsite(vsName, aHandle, ts)
    

    def notify_vsite(self, vsName, aHandle, ts):
        """Tell slave to notify a vSite to update to the specified time."""
        aHandle.logger.debug("Connection notify for %s sent to %s",
                             vsName, repr(self._addr))
        if vsName in self._vSites:
            p = xdrlib.Packer()
            p.pack_string(vsName)
            p.pack_string(ts)
            self._wbuf += p.get_buffer()
            aHandle.logger.debug("Connection to %s has %d bytes queued",
                                 repr(self._addr), len(self._wbuf))

    def __hash__(self):
        return hash(self._s)



class SlaveAgentSet(ConnectionSet):
    """Model interactions with slave agents."""

    def __init__(self, listenAddr):
        ConnectionSet.__init__(self)

        # Open a socket for listening to connection requests.
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(listenAddr)
        s.listen(5)
        self._s = s

    def prepare_select(self, r, w):
        """Prepare lists for select."""

        ConnectionSet.prepare_select(self, r, w)

        # Always listen to the connection socket.
        r.append(self._s)


    def read_if_ready(self, r, aHandle):
        """Read from whatever connections are ready."""

        ConnectionSet.read_if_ready(self, r, aHandle)

        logger = aHandle.logger

        # See if there is a connection request.
        if self._s in r:
            try:
                s,addr = self._s.accept()
                s.setblocking(0)
            except:
                logger.exception("Failed to accept new slave connection.")
            else:
                logger.info("Accepted connection from slave.")
                c = SlaveAgentConnection(s, addr, aHandle)
                self.insert_connection(c)


    def notify_vsite(self, vsName, aHandle):
        """Notify slaves that a vsite needs updating."""

        ts = aHandle.vHandles[vsName].get_timestamp()
        aHandle.logger.debug("Notify agents about vsite %s", vsName)
        for c in self._conns.values():
            c.notify_vsite(vsName, aHandle, ts)

