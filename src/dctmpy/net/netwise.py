# Copyright (c) 2013 Andrey B. Panfilov <andrew@panfilov.tel>
#
# See main module for license.
#
try:
    from OpenSSL import SSL, crypto
except:
    pass
import socket
import ssl


class Netwise(object):
    attributes = ['version', 'release', 'inumber', 'sequence', 'host', 'port', 'secure', 'sslopts', 'socket']

    def __init__(self, **kwargs):
        for attribute in Netwise.attributes:
            setattr(self, attribute, kwargs.pop(attribute, None))
        if self.sslopts is None:
            self.sslopts = kwargs
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
                if not self.host or not (self.port > -1):
                    raise RuntimeError("Invalid host or port")
                self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                if self.secure:
                    if SSL:
                        ctx = SSL.Context(SSL.SSLv23_METHOD)
                        if self.sslopts and self.sslopts.get("ciphers", None):
                            ctx.set_cipher_list(self.sslopts.get("ciphers"))
                        self.socket = SSL.Connection(ctx, socket.socket(socket.AF_INET, socket.SOCK_STREAM))
                    else:
                        self.socket = ssl.wrap_socket(self.socket, **dict(self.sslopts))
                self.socket.connect((self.host, self.port))
            except Exception, e:
                if self.secure:
                    try:
                        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        self.socket.connect((self.host, self.port - 1))
                        self.port -= 1
                        self.secure = False
                    except:
                        self.socket = None
                        raise e
                else:
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
        sequence = kwargs.get('sequence', None)
        if sequence is None:
            sequence = self.sequence = self.sequence + 1
        return cls(**dict(kwargs, **{
            'socket': self._socket(),
            'sequence': sequence,
            'version': self.version,
            'release': self.release,
            'inumber': self.inumber,
        })).receive()
