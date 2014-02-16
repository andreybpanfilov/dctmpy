#  Copyright (c) 2013 Andrey B. Panfilov <andrew@panfilov.tel>
#
#  See main module for license.
#

from dctmpy import *
from dctmpy.net.request import Request
import socket


class Netwise(object):
    fields = ['version', 'release', 'inumber', 'sequence', 'sockopts']

    def __init__(self, **kwargs):
        for attribute in Netwise.fields:
            self.__setattr__(ATTRIBUTE_PREFIX + attribute, kwargs.pop(attribute, None))
        if self.sockopts is None:
            self.sockopts = kwargs
        if self.sequence is None:
            self.sequence = 0
        self.__socket = None

    def connected(self):
        if self.__socket is None:
            return False
        return True

    def socket(self):
        if not self.connected():
            try:
                host = self.sockopts.get('host', None)
                port = self.sockopts.get('port', None)
                if host is None or port is None:
                    raise RuntimeError("Invalid host or port")
                self.__socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.__socket.connect((host, port))
            except:
                self.__socket = None
                raise
        return self.__socket

    def disconnect(self):
        try:
            if self.connected():
                self.__socket.close()
        finally:
            self.__socket = None

    def __del__(self):
        self.disconnect()

    def request(self, **kwargs):
        return Request(**dict(kwargs, **{
            'socket': self.socket(),
            'sequence': ++self.sequence,
            'version': self.version,
            'release': self.release,
            'inumber': self.inumber,
        }))

    def __getattr__(self, name):
        if name in Netwise.fields:
            return self.__getattribute__(ATTRIBUTE_PREFIX + name)
        else:
            raise AttributeError

    def __setattr__(self, name, value):
        if name in Netwise.fields:
            Netwise.__setattr__(self, ATTRIBUTE_PREFIX + name, value)
        else:
            super(Netwise, self).__setattr__(name, value)

