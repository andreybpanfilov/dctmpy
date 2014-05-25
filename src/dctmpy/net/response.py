#  Copyright (c) 2013 Andrey B. Panfilov <andrew@panfilov.tel>
#
#  See main module for license.
#

from dctmpy.net import *


class Response(object):
    attributes = ['message', 'data']

    def __init__(self, **kwargs):
        for attribute in Response.attributes:
            setattr(self, attribute, kwargs.pop(attribute, None))

        if self.message is None:
            raise ProtocolException("Response undefined")

        message = array.array('B')
        message.extend(self.message)
        self.data = []

        self._deserialize()

    def _deserialize(self):
        while len(self.message) > 0:
            if self.message[0] == INTEGER_START:
                self.data.append(self._read_integer())
            elif self.message[0] == EMPTY_STRING_START and self.message[0] == NULL_BYTE:
                self.data.append(self._read_string())
            elif self.message[0] == STRING_START:
                self.data.append(self._read_string())
            elif self.message[0] == STRING_ARRAY_START and self.message[1] == 0x80:
                self.data.append(self._read_string())
            elif self.message[0] == INT_ARRAY_START:
                self.data.append(self._read_integer_array())

    def _read_string(self):
        return read_string(self.message)

    def _read_integer(self):
        return read_integer(self.message)

    def _read_integer_array(self):
        return read_integer_array(self.message)

    def next(self):
        if len(self.data) > 0:
            return self.data.pop(0)
        return None

    def last(self):
        if len(self.data) > 0:
            return self.data.pop()
        return None

    def __getattr__(self, name):
        if name in Response.attributes:
            return self.__getattribute__(ATTRIBUTE_PREFIX + name)
        else:
            return AttributeError("Unknown attribute %s in %s" % (name, str(self.__class__)))

    def __setattr__(self, name, value):
        if name in Response.attributes:
            Response.__setattr__(self, ATTRIBUTE_PREFIX + name, value)
        else:
            super(Response, self).__setattr__(name, value)


class DownloadResponse(Response):
    def __init__(self, **kwargs):
        super(DownloadResponse, self).__init__(**kwargs)

    def _read_string(self):
        return read_binary(self.message)
