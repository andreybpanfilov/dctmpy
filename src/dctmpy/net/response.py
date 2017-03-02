# Copyright (c) 2013 Andrey B. Panfilov <andrew@panfilov.tel>
#
# See main module for license.
#
from dctmpy.exceptions import ProtocolException
from dctmpy.net import *


class Response(object):
    attributes = ['message', 'offset']

    def __init__(self, **kwargs):
        for attribute in Response.attributes:
            setattr(self, attribute, kwargs.pop(attribute, None))

        if self.message is None:
            raise ProtocolException("Response undefined")

    def _read_string(self):
        (result, self.offset) = read_string(self.message, self.offset)
        return buffer(result)

    def _read_integer(self):
        (result, self.offset) = read_integer(self.message, self.offset)
        return result

    def _read_integer_array(self):
        (result, self.offset) = read_integer_array(self.message, self.offset)
        return result

    def next(self):
        if len(self.message) <= self.offset:
            return None
        seq1 = self.message[self.offset]
        seq2 = self.message[1 + self.offset]
        if seq1 == INTEGER_START:
            return self._read_integer()
        if seq1 == EMPTY_STRING_START and seq2 == NULL_BYTE:
            return self._read_string()
        if seq1 == STRING_START:
            return self._read_string()
        if seq1 == STRING_ARRAY_START and seq2 == 0x80:
            return self._read_string()
        if seq1 == INT_ARRAY_START:
            return self._read_integer_array()
        raise RuntimeError("Unknown sequence")


class DownloadResponse(Response):
    def __init__(self, **kwargs):
        super(DownloadResponse, self).__init__(**kwargs)

    def _read_string(self):
        (result, self.offset) = read_binary(self.message, self.offset)
        return result


class UploadResponse(Response):
    attributes = ['rpc', 'sequence']

    def __init__(self, **kwargs):
        for attribute in UploadResponse.attributes:
            setattr(self, attribute, kwargs.pop(attribute, None))
        super(UploadResponse, self).__init__(**kwargs)
