#  Copyright (c) 2013 Andrey B. Panfilov <andrew@panfilov.tel>
#
#  See main module for license.
#

import array
from dctmpy import *
from dctmpy.net import *
from dctmpy.net.response import Response

HEADER_SIZE = 4


class Request(object):
    attrs = ['version', 'release', 'inumber', 'sequence', 'socket', 'immediate', 'type']

    def __init__(self, **kwargs):
        for attribute in Request.attrs:
            self.__setattr__(ATTRIBUTE_PREFIX + attribute, kwargs.pop(attribute, None))

        if self.version is None or self.release is None or self.inumber is None:
            raise ProtocolException("Wrong protocol version info")

        if self.type is None:
            raise ProtocolException("Invalid request type")

        data = kwargs.pop('data', None)

        if data is None:
            self.data = None
        else:
            self.data = serializeData(data)

        if self.immediate:
            self.send()

    def send(self):
        self.socket.sendall(
            self.buildRequest()
        )

    def receive(self):
        messagePayload = array.array('B')
        messagePayload.fromstring(self.socket.recv(HEADER_SIZE))
        if len(messagePayload) == 0:
            raise ProtocolException("Unable to read header")

        messageLength = 0
        for i in xrange(0, HEADER_SIZE):
            messageLength = messageLength << 8 | messagePayload[i]

        headerPayload = stringToIntegerArray(self.socket.recv(2))
        if headerPayload[0] != PROTOCOL_VERSION:
            raise ProtocolException("Wrong protocol 0x%X expected 0x%X" % (headerPayload[0], PROTOCOL_VERSION))
        headerLength = headerPayload[1]

        header = stringToIntegerArray(self.socket.recv(headerLength))

        sequence = readInteger(header)
        if sequence != self.sequence:
            raise ProtocolException("Invalid sequence %d expected %d" % (sequence, self.sequence))

        status = readInteger(header)
        if status != 0:
            raise ProtocolException("Bad status: 0x%X" % status)

        bytesToRead = messageLength - len(headerPayload) - headerLength
        message = array.array('B')
        while True:
            chunk = stringToIntegerArray(self.socket.recv(bytesToRead))
            message.extend(chunk)
            if len(chunk) == 0 or len(message) == bytesToRead:
                break

        return Response(**{
            'message': message
        })

    def buildRequest(self):
        data = self.buildHeader()
        if self.data is not None:
            data.extend(self.data)
        length = len(data)
        data.insert(0, length & 0x000000ff)
        data.insert(0, (length >> 8) & 0x000000ff)
        data.insert(0, (length >> 16) & 0x000000ff)
        data.insert(0, (length >> 24) & 0x000000ff)
        return data

    def buildHeader(self):
        header = array.array('B')
        header.extend(serializeInteger(self.sequence))
        header.extend(serializeInteger(self.type))
        header.extend(serializeInteger(self.version))
        header.extend(serializeInteger(self.release))
        header.extend(serializeInteger(self.inumber))
        header.insert(0, len(header))
        header.insert(0, PROTOCOL_VERSION)
        return header

    def __getattr__(self, name):
        if name in Request.attrs:
            return self.__getattribute__(ATTRIBUTE_PREFIX + name)
        else:
            return AttributeError

    def __setattr__(self, name, value):
        if name in Request.attrs:
            Request.__setattr__(self, ATTRIBUTE_PREFIX + name, value)
        else:
            super(Request, self).__setattr__(name, value)



