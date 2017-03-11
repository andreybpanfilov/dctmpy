# Copyright (c) 2013 Andrey B. Panfilov <andrew@panfilov.tel>
#
#  See main module for license.
#

from dctmpy import *
from dctmpy.exceptions import ParserException
from dctmpy.obj.attrvalue import AttrValue
from dctmpy.obj.typedobject import TypedObject


class DocbrokerObject(TypedObject):
    def __init__(self, **kwargs):
        super(DocbrokerObject, self).__init__(**dict(
            kwargs,
            **{'ser_version': 0}
        ))

    def _read_type(self):
        pass

    def _read_object(self):
        header = self._next_token()
        if "OBJ" != header:
            raise ParserException("Invalid header, expected OBJ, got: %s" % header)

        type_name = self._next_token()
        if is_empty(type_name):
            raise ParserException("Wrong type name")

        self._read_int()

        for i in xrange(0, self._read_int()):
            self._read_attr(i)

    def _read_attr(self, index):
        attr_name = self._next_string(ATTRIBUTE_PATTERN)
        attr_type = self._next_string(ATTRIBUTE_PATTERN)
        repeating = self._next_string(REPEATING_PATTERN) == REPEATING
        attr_length = self._read_int()

        if attr_type is None:
            raise ParserException("Unknown type")

        result = []

        if not repeating:
            result.append(self._read_attr_value(attr_type))
        else:
            for i in xrange(0, self._read_int()):
                result.append(self._read_attr_value(attr_type))

        self.add(AttrValue(**{
            'name': attr_name,
            'type': attr_type,
            'length': attr_length,
            'values': result,
            'repeating': repeating,
            'extended': False,
        }))


class DocbaseMap(DocbrokerObject):
    def __init__(self, **kwargs):
        super(DocbaseMap, self).__init__(**kwargs)

    def add(self, value):
        if not value.name in ['i_host_addr']:
            value.repeating = True
        super(DocbrokerObject, self).add(value)
