# Copyright (c) 2013 Andrey B. Panfilov <andrew@panfilov.tel>
#
# See main module for license.
#

import socket
import ssl


class Netwise(object):
    attributes = ['version', 'release', 'inumber', 'sequence', 'sockopts', 'socket']

    def __init__(self, **kwargs):
        for attribute in Netwise.attributes:
            setattr(self, attribute, kwargs.pop(attribute, None))
        if self.sockopts is None:
            self.sockopts = kwargs
        if self.sequence is None:
            self.sequence = 0
        self.socket = None

    def _connected(self):
        if not self.socket:
            return False
        return True

    def _socket(self):
        if not self._connected():
            try:
                host = self.sockopts.get('host', None)
                port = self.sockopts.get('port', None)
                secure = self.sockopts.get('secure', False)
                if not host or not (port > -1):
                    raise RuntimeError("Invalid host or port")
                self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                if secure:
                    self.socket = ssl.wrap_socket(self.socket)
                self.socket.connect((host, port))
            except:
                self.socket = None
                raise
        return self.socket

    def disconnect(self):
        try:
            if self._connected():
                self.socket.close()
        finally:
            self.socket = None

    def __del__(self):
        self.disconnect()

    def request(self, cls, **kwargs):
        return cls(**dict(kwargs, **{
            'socket': self._socket(),
            'sequence': ++self.sequence,
            'version': self.version,
            'release': self.release,
            'inumber': self.inumber,
        }))


