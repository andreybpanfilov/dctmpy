# Copyright (c) 2013 Andrey B. Panfilov <andrew@panfilov.tel>
#
# See main module for license.
#
from dctmpy.exceptions import ProtocolException
from dctmpy.net import *
from dctmpy.net.response import Response, DownloadResponse, UploadResponse

HEADER_SIZE = 4
BUFFER_SIZE = 65536


class Request(object):
    attributes = ['version', 'release', 'inumber', 'sequence', 'socket', 'type']

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

        self.send()

    def send(self):
        self.socket.sendall(
            str(self._build_request())
        )

    def receive(self):
        return self._receive(Response)

    def _receive(self, cls):
        message = bytearray(self.socket.recv(max(BUFFER_SIZE, HEADER_SIZE + 2)))
        if len(message) < HEADER_SIZE + 2:
            raise ProtocolException("Unable to read header")

        message_length = 0
        for i in xrange(0, HEADER_SIZE):
            message_length = message_length << 8 | message[i]

        if message[HEADER_SIZE] != PROTOCOL_VERSION:
            raise ProtocolException("Wrong protocol 0x%X expected 0x%X" % (message[HEADER_SIZE], PROTOCOL_VERSION))
        header_length = message[HEADER_SIZE + 1]

        while len(message) < HEADER_SIZE + 2 + header_length:
            message.extend(self.socket.recv(BUFFER_SIZE))

        (sequence, offset) = read_integer(message, HEADER_SIZE + 2)
        if sequence != self.sequence:
            raise ProtocolException("Invalid sequence %d expected %d" % (sequence, self.sequence))

        (status, offset) = read_integer(message, offset)
        if status != 0:
            raise ProtocolException("Bad status: 0x%X" % status)

        bytes_to_read = message_length - len(message) + HEADER_SIZE
        while bytes_to_read > 0:
            chunk = bytearray(self.socket.recv(bytes_to_read))
            bytes_to_read -= len(chunk)
            message.extend(chunk)
            if len(chunk) == 0:
                break
        return cls(**{
            'message': message,
            'offset': offset
        })

    def _build_request(self):
        data = bytearray(4)
        data.extend(self._build_header())
        if self.data:
            data.extend(self.data)
        length = len(data) - 4
        data[3] = length & 0x000000ff
        data[2] = (length >> 8) & 0x000000ff
        data[1] = (length >> 16) & 0x000000ff
        data[0] = (length >> 24) & 0x000000ff
        return data

    def _build_header(self):
        header = bytearray(2)
        header.extend(serialize_integer(self.sequence))
        header.extend(serialize_integer(self.type))
        header.extend(serialize_integer(self.version))
        header.extend(serialize_integer(self.release))
        header.extend(serialize_integer(self.inumber))
        header[1] = len(header) - 2
        header[0] = PROTOCOL_VERSION
        return header


class DownloadRequest(Request):
    def __init__(self, **kwargs):
        super(DownloadRequest, self).__init__(**kwargs)

    def receive(self):
        return self._receive(DownloadResponse)


class UploadRequest(Request):
    def __init__(self, **kwargs):
        super(UploadRequest, self).__init__(**kwargs)

    def receive(self):
        return self._receive(UploadResponse)

    def _build_header(self):
        header = bytearray(2)
        header.extend(serialize_integer(self.sequence))
        header.extend(serialize_integer(self.type))
        # here we actually act as server
        if self.type > 0:
            header.extend(serialize_integer(self.version))
            header.extend(serialize_integer(self.release))
            header.extend(serialize_integer(self.inumber))
        header[1] = len(header) - 2
        header[0] = PROTOCOL_VERSION
        return header

    def _receive(self, cls):
        message = bytearray(self.socket.recv(max(BUFFER_SIZE, HEADER_SIZE + 2)))
        if len(message) < HEADER_SIZE + 2:
            raise ProtocolException("Unable to read header")

        message_length = 0
        for i in xrange(0, HEADER_SIZE):
            message_length = message_length << 8 | message[i]

        if message[HEADER_SIZE] != PROTOCOL_VERSION:
            raise ProtocolException("Wrong protocol 0x%X expected 0x%X" % (message[HEADER_SIZE], PROTOCOL_VERSION))
        header_length = message[HEADER_SIZE + 1]

        while len(message) < HEADER_SIZE + 2 + header_length:
            message.extend(self.socket.recv(BUFFER_SIZE))

        (sequence, offset) = read_integer(message, HEADER_SIZE + 2)

        (rpc, offset) = read_integer(message, offset)
        if rpc not in CHUNKS:
            raise ProtocolException("Unknown callback rpc: 0x%X" % rpc)

        bytes_to_read = message_length - len(message) + HEADER_SIZE
        while bytes_to_read > 0:
            chunk = bytearray(self.socket.recv(bytes_to_read))
            bytes_to_read -= len(chunk)
            message.extend(chunk)
            if len(chunk) == 0:
                break

        return cls(**{
            'sequence': sequence,
            'rpc': rpc,
            'message': message,
            'offset': offset
        })
