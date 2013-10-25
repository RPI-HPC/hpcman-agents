"""Connections and sets of connections."""

__all__ = [
    "Connection", "ConnectionSet",
    "ConnectionError"
    ]


class ConnectionError(Exception):
    pass


class Connection(object):
    """Model a connection to a remote peer."""

    def __init__(self, addr, aHandle):
        self._addr = addr
        self._s = None
        self._rbuf = ""
        self._wbuf = ""


    def handle_disconnect(self, aHandle):
        """Handle disconnection to peer."""

        raise NotImplementedError


    def handle_input(self, aHandle):
        """Process input."""

        raise NotImplementedError


    def prepare_select(self, r, w):
        """Prepare selection sets for readers, writers."""

        if self._s is not None:
            r.append(self._s)
        if len(self._wbuf) > 0:
            w.append(self._s)

    def write_if_ready(self, w, aHandle):
        s = self._s
        if s not in w:
            return
        wbuf = self._wbuf
        assert len(wbuf) > 0
        try:
            sent = s.send(wbuf)
        except:
            self.handle_disconnect(aHandle)
            return
        self._wbuf = wbuf[sent:]
        aHandle.logger.debug("Wrote to peer at %s: %s (%d of %d bytes)",
                                 repr(self._addr), repr(wbuf[sent:]), sent, len(wbuf))

    def read_if_ready(self, r, aHandle):
        """Process pending input, or disconnection."""
        s = self._s
        if s not in r:
            return
        logger = aHandle.logger

        # Get the latest data.
        try:
            # FIXME: Configurable buffer size (?)
            rbuf = s.recv(4096)
        except:
            # Something weird with this socket!
            logger.exception("Exception thrown, socket to %s",
                             repr(self._addr))
            self.handle_disconnect(aHandle)
            return
        if len(rbuf) == 0:
            logger.info("Connection to %s was closed", repr(self._addr))
            self.handle_disconnect(aHandle)
            return
        self._rbuf += rbuf
        self.handle_input(aHandle)

    def work_if_ready(self, now, aHandle):
        """See if any deferred work is scheduled by `now`."""

        raise NotImplementedError


class ConnectionSet(object):
    """Model interactions with sets of connections."""

    def __init__(self):
        self._conns = {}

    def prepare_select(self, r, w):
        """Prepare selection sets for readers, writers."""

        for c in self._conns.values():
            c.prepare_select(r, w)

    def insert_connection(self, c):
        """Insert a new managed connection."""

        self._conns[c] = c


    def write_if_ready(self, w, aHandle):
        """Write to whatever connections are ready."""

        for c in self._conns.values():
            try:
                c.write_if_ready(w, aHandle)
            except ConnectionError:
                del self._conns[c]


    def read_if_ready(self, r, aHandle):
        """Read from whatever connections are ready."""

        for c in self._conns.values():
            try:
                c.read_if_ready(r, aHandle)
            except ConnectionError:
                del self._conns[c]

    def work_if_ready(self, now, aHandle):
        """Attempt all scheduled connections."""

        for c in self._conns.values():
            c.work_if_ready(now, aHandle)
