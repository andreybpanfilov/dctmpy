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
            **{'serializationversion': 0}
        ))

    def readType(self):
        pass

    def readObject(self):
        header = self.nextToken()
        if "OBJ" != header:
            raise ParserException("Invalid header, expected OBJ, got: %s" % header)

        typename = self.nextToken()
        if isEmpty(typename):
            raise ParserException("Wrong type name")

        self.readInt()

        for i in xrange(0, self.readInt()):
            self.readAttr(i)

    def readAttr(self, index):
        attrName = self.nextString(ATTRIBUTE_PATTERN)
        attrType = self.nextString(ATTRIBUTE_PATTERN)
        repeating = self.nextString(REPEATING_PATTERN) == REPEATING
        attrLength = self.readInt()

        if attrType is None:
            raise ParserException("Unknown type")

        result = []

        if not repeating:
            result.append(self.readAttrValue(attrType))
        else:
            for i in xrange(0, self.readInt()):
                result.append(self.readAttrValue(attrType))

        self.add(AttrValue(**{
            'name': attrName,
            'type': attrType,
            'length': attrLength,
            'values': result,
            'repeating': repeating,
        }))


class DocbaseMap(DocbrokerObject):
    def __init__(self, **kwargs):
        super(DocbaseMap, self).__init__(**kwargs)

    def add(self, attrValue):
        if not attrValue.name in ['i_host_addr']:
            attrValue.repeating = True
        super(DocbrokerObject, self).add(attrValue)
