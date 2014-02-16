#  Copyright (c) 2013 Andrey B. Panfilov <andrew@panfilov.tel>
#
#  See main module for license.
#

from dctmpy import *
from dctmpy.obj.typedobject import TypedObject


class DocbrokerObject(TypedObject):
    def __init__(self, **kwargs):
        super(DocbrokerObject, self).__init__(**dict(
            kwargs,
            **{'serversion': 0}
        ))

    def __read_type__(self):
        pass

    def __read_object__(self):
        header = self.__next_token__()
        if "OBJ" != header:
            raise ParserException("Invalid header, expected OBJ, got: %s" % header)

        type_name = self.__next_token__()
        if isempty(type_name):
            raise ParserException("Wrong type name")

        self.__read_int__()

        for i in xrange(0, self.__read_int__()):
            self.__read_attr__(i)

    def __read_attr__(self, index):
        attr_name = self.__next_string__(ATTRIBUTE_PATTERN)
        attr_type = self.__next_string__(ATTRIBUTE_PATTERN)
        repeating = self.__next_string__(REPEATING_PATTERN) == REPEATING
        attr_length = self.__read_int__()

        if attr_type is None:
            raise ParserException("Unknown type")

        result = []

        if not repeating:
            result.append(self.__read_attr_value__(attr_type))
        else:
            for i in xrange(0, self.__read_int__()):
                result.append(self.__read_attr_value__(attr_type))

        self.add(AttrValue(**{
            'name': attr_name,
            'type': attr_type,
            'length': attr_length,
            'values': result,
            'repeating': repeating,
        }))


class DocbaseMap(DocbrokerObject):
    def __init__(self, **kwargs):
        super(DocbaseMap, self).__init__(**kwargs)

    def add(self, value):
        if not value.name in ['i_host_addr']:
            value.repeating = True
        super(DocbrokerObject, self).add(value)
