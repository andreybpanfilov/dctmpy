#  Copyright (c) 2013 Andrey B. Panfilov <andrew@panfilov.tel>
#
#  See main module for license.
#

from dctmpy.net import *
from dctmpy.net.response import Response, DownloadResponse

HEADER_SIZE = 4


class Request(object):
    attributes = ['version', 'release', 'inumber', 'sequence', 'socket', 'immediate', 'type']

    def __init__(self, **kwargs):
        for attribute in Request.attributes:
            setattr(self, attribute, kwargs.pop(attribute, None))

        if self.version is None or self.release is None or self.inumber is None:
            raise ProtocolException("Wrong protocol version info")

        if self.type is None:
            raise ProtocolException("Invalid request type")

        data = kwargs.pop('data', None)

        if data is None:
            self.data = None
        else:
            self.data = serialize_data(data)

        if self.immediate:
            self.send()

    def send(self):
        self.socket.sendall(
            self._build_request()
        )

    def receive(self):
        return self._receive(Response)

    def _receive(self, cls):
        message_payload = array.array('B')
        message_payload.fromstring(self.socket.recv(HEADER_SIZE))
        if len(message_payload) == 0:
            raise ProtocolException("Unable to read header")

        message_length = 0
        for i in xrange(0, HEADER_SIZE):
            message_length = message_length << 8 | message_payload[i]

        header_payload = string_to_integer_array(self.socket.recv(2))
        if header_payload[0] != PROTOCOL_VERSION:
            raise ProtocolException("Wrong protocol 0x%X expected 0x%X" % (header_payload[0], PROTOCOL_VERSION))
        header_length = header_payload[1]

        header = string_to_integer_array(self.socket.recv(header_length))

        sequence = read_integer(header)
        if sequence != self.sequence:
            raise ProtocolException("Invalid sequence %d expected %d" % (sequence, self.sequence))

        status = read_integer(header)
        if status != 0:
            raise ProtocolException("Bad status: 0x%X" % status)

        bytes_to_read = message_length - len(header_payload) - header_length
        message = array.array('B')
        while True:
            chunk = string_to_integer_array(self.socket.recv(bytes_to_read))
            message.extend(chunk)
            if len(chunk) == 0 or len(message) == bytes_to_read:
                break

        return cls(**{
            'message': message
        })

    def _build_request(self):
        data = self._build_header()
        if self.data:
            data.extend(self.data)
        length = len(data)
        data.insert(0, length & 0x000000ff)
        data.insert(0, (length >> 8) & 0x000000ff)
        data.insert(0, (length >> 16) & 0x000000ff)
        data.insert(0, (length >> 24) & 0x000000ff)
        return data

    def _build_header(self):
        header = array.array('B')
        header.extend(serialize_integer(self.sequence))
        header.extend(serialize_integer(self.type))
        header.extend(serialize_integer(self.version))
        header.extend(serialize_integer(self.release))
        header.extend(serialize_integer(self.inumber))
        header.insert(0, len(header))
        header.insert(0, PROTOCOL_VERSION)
        return header

    def __getattr__(self, name):
        if name in Request.attributes:
            return self.__getattribute__(ATTRIBUTE_PREFIX + name)
        else:
            return AttributeError("Unknown attribute %s in %s" % (name, str(self.__class__)))

    def __setattr__(self, name, value):
        if name in Request.attributes:
            Request.__setattr__(self, ATTRIBUTE_PREFIX + name, value)
        else:
            super(Request, self).__setattr__(name, value)


class DownloadRequest(Request):
    def __init__(self, **kwargs):
        super(DownloadRequest, self).__init__(**kwargs)

    def receive(self):
        return self._receive(DownloadResponse)