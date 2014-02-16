#  Copyright (c) 2013 Andrey B. Panfilov <andrew@panfilov.tel>
#
#  See main module for license.
#

import array
from dctmpy.net import *


class Response(object):
    def __init__(self, **kwargs):
        message = kwargs.pop('message', None)

        if message is None:
            raise ProtocolException("Response undefined")

        self.__message = array.array('B')
        self.__message.extend(message)
        self.__data = []

        self.deserialize()

    def deserialize(self):
        while len(self.__message) > 0:
            if self.__message[0] == INTEGER_START:
                self.__data.append(readInteger(self.__message))
            elif self.__message[0] == EMPTY_STRING_START and self.__message[0] == NULL_BYTE:
                self.__data.append(readString(self.__message))
            elif self.__message[0] == STRING_START:
                self.__data.append(readString(self.__message))
            elif self.__message[0] == STRING_ARRAY_START and self.__message[1] == 0x80:
                self.__data.append(readString(self.__message))
            elif self.__message[0] == INT_ARRAY_START:
                self.__data.append(readIntegerArray(self.__message))

    def next(self):
        if len(self.__data) > 0:
            return self.__data.pop(0)
        return None

    def last(self):
        if len(self.__data) > 0:
            return self.__data.pop()
        return None