#  Copyright (c) 2013 Andrey B. Panfilov <andrew@panfilov.tel>
#
#  See main module for license.
#

import re
from dctmpy import *


class TypedObject(object):
    fields = ['session', 'type', 'buffer', 'serializationversion', 'iso8601time']

    def __init__(self, **kwargs):
        for attribute in TypedObject.fields:
            self.__setattr__(ATTRIBUTE_PREFIX + attribute, kwargs.pop(attribute, None))
        self.__attrs = {}

        if self.serializationversion is None:
            self.serializationversion = self.session.serializationversion

        if self.iso8601time is None:
            if self.serializationversion == 2:
                self.iso8601time = self.session.iso8601time
            else:
                self.iso8601time = False

        if not isEmpty(self.buffer):
            self.read()

    def read(self, buf=None):
        if isEmpty(buf) and isEmpty(self.buffer):
            raise ParserException("Empty data")
        elif not isEmpty(buf):
            self.buffer = buf

        self.readHeader()

        if self.type is None and self.needReadType():
            self.type = self.readType()

        if self.needReadObject():
            self.readObject()

    def readHeader(self):
        if self.serializationversion > 0:
            serializationversion = self.readInt()
            if serializationversion != self.serializationversion:
                raise RuntimeError(
                    "Invalid serialization version %d, expected %d" % (serializationversion, self.serializationversion))

    def readType(self):
        header = self.nextToken()
        if header != "TYPE":
            raise ParserException("Invalid type header: %s" % header)

        typeInfo = self.readTypeInfo()
        for i in xrange(0, self.readInt()):
            typeInfo.append(self.readAttrInfo())

        return typeInfo

    def readObject(self):
        header = self.nextToken()
        if "OBJ" != header:
            raise ParserException("Invalid header, expected OBJ, got: %s" % header)

        typename = self.nextToken()

        if typename is None or len(typename) == 0:
            raise ParserException("Wrong type name")

        if self.serializationversion > 0:
            self.readInt()
            self.readInt()
            self.readInt()

        if self.type is None or typename != self.type.name:
            raise ParserException("No type info for %s" % typename)

        for i in xrange(0, self.readInt()):
            self.readAttr(i)

        self.readExtendedAttr()

    def readAttr(self, index):
        position = self.ifd6(self.readBase64Int)
        if position is None:
            position = index

        repeating = self.type.get(position).repeating
        attrType = self.type.get(position).type

        if self.serializationversion == 2:
            repeating = self.nextString(REPEATING_PATTERN) == REPEATING
            attrType = TYPES[self.readInt()]

        attrName = self.type.get(position).name
        attrLength = self.type.get(position).length

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
            'position': position,
            'type': attrType,
            'length': attrLength,
            'values': result,
            'repeating': repeating,
        }))

    def add(self, attrValue):
        self.__attrs[attrValue.name] = attrValue

    def readExtendedAttr(self):
        attrCount = self.readInt()
        for i in xrange(0, attrCount):
            attrName = self.nextString(ATTRIBUTE_PATTERN)
            attrType = self.nextString(ATTRIBUTE_PATTERN)
            repeating = REPEATING == self.nextString()
            length = self.readInt()

            if isEmpty(attrType):
                raise ParserException("Unknown typedef: %s" % attrType)

            result = []

            if not repeating:
                result.append(self.readAttrValue(attrType))
            else:
                for i in xrange(1, self.readInt()):
                    result.append(self.readAttrValue(attrType))

            self.__attrs[attrName] = AttrValue(**{
                'name': attrName,
                'type': attrType,
                'length': length,
                'values': result,
                'repeating': repeating,
            })

    def readAttrValue(self, attrType):
        return {
            INT: lambda: self.readInt(),
            STRING: lambda: self.readString(),
            TIME: lambda: self.readTime(),
            BOOL: lambda: self.readBoolean(),
            ID: lambda: self.nextString(),
            DOUBLE: lambda: self.nextString(),
            UNDEFINED: lambda: self.nextString()
        }[attrType]()

    def readTypeInfo(self):
        return TypeInfo(**{
            'name': self.nextString(ATTRIBUTE_PATTERN),
            'id': self.nextString(ATTRIBUTE_PATTERN),
            'vstamp': self.ifd6(self.readInt),
            'version': self.ifd6(self.readInt),
            'cache': self.ifd6(self.readInt),
            'super': self.nextString(ATTRIBUTE_PATTERN),
            'sharedparent': self.ifd6(self.nextString, None, ATTRIBUTE_PATTERN),
            'aspectname': self.ifd6(self.nextString, None, ATTRIBUTE_PATTERN),
            'aspectshareflag': self.ifd6(self.readBoolean),
            'serializationversion': self.serializationversion,
        })

    def readAttrInfo(self):
        return AttrInfo(**{
            'position': self.ifd6(self.readBase64Int),
            'name': self.nextString(ATTRIBUTE_PATTERN),
            'type': self.nextString(TYPE_PATTERN),
            'repeating': REPEATING == self.nextString(),
            'length': self.readInt(),
            'restriction': self.ifd6(self.readInt),
        })

    def ifd6(self, method, default=None, *args, **kwargs):
        if self.serializationversion > 0:
            return method(*args, **kwargs)
        return default

    def serialize(self):
        result = ""
        if self.serializationversion > 0:
            result += "%d\n" % self.serializationversion
        result += "OBJ NULL 0 "
        if self.serializationversion > 0:
            result += "0 0\n0\n"
        result += "%d\n" % len(self.__attrs)
        for attrValue in self.__attrs.values():
            result += "%s %s %s %d\n" % (
                attrValue.name, attrValue.type, [SINGLE, REPEATING][attrValue.repeating],
                attrValue.length)
            if attrValue.repeating:
                result += "%d\n" % len(attrValue.values)
            for value in attrValue.values:
                if STRING == attrValue.type:
                    result += "A %d %s\n" % (len(value), value)
                elif BOOL == attrValue.type:
                    result += "%s\n" % ["F", "T"][value]
                else:
                    result += "%s\n" % value
        return result

    def needReadType(self):
        return True

    def needReadObject(self):
        return True

    def substr(self, length):
        data = self.buffer
        self.buffer = data[length:]
        return data[:length]

    def nextToken(self, separator=DEFAULT_SEPARATOR):
        self.buffer = re.sub("^%s" % separator, "", self.buffer)
        m = re.search(separator, self.buffer)
        if m is not None:
            return self.substr(m.start(0))
        else:
            return self.substr(len(self.buffer))

    def nextString(self, pattern=None, separator=DEFAULT_SEPARATOR):
        value = self.nextToken(separator)
        if pattern is not None:
            if re.match(pattern, value) is None:
                raise ParserException("Invalid string: %s for regexp %s" % (value, pattern))
        return value

    def readInt(self):
        return int(self.nextString(INTEGER_PATTERN))

    def readBase64Int(self):
        return pseudoBase64ToInt(self.nextString(BASE64_PATTERN))

    def readString(self):
        self.nextString(ENCODING_PATTERN)
        return self.substr(self.readInt() + 1)[1:]

    def readTime(self):
        value = self.nextToken(CRLF_PATTERN)
        if value.startswith(" "):
            value = value[1:]
        if value.startswith("xxx "):
            value = value[4:]
        return parseTime(value, self.iso8601time)

    def readBoolean(self):
        return bool(self.nextString(BOOLEAN_PATTERN))

    def getAttr(self, attrName):
        if attrName in self.__attrs:
            return self.__attrs[attrName]
        else:
            raise RuntimeError("No attribute %s" % attrName)

    def __getattr__(self, name):
        if name in self.__attrs:
            return self.__attrs[name]
        elif name in TypedObject.fields:
            return self.__getattribute__(ATTRIBUTE_PREFIX + name)
        else:
            raise AttributeError

    def __setattr__(self, name, value):
        if name in TypedObject.fields:
            TypedObject.__setattr__(self, ATTRIBUTE_PREFIX + name, value)
        else:
            super(TypedObject, self).__setattr__(name, value)

    def __len__(self):
        return len(self.__attrs)

    def __contains__(self, key):
        return key in self.__attrs

    def __getitem__(self, key):
        if key in self.__attrs:
            attrValue = self.__attrs[key]
            if attrValue.repeating:
                return attrValue.values
            else:
                return attrValue[0]
        else:
            raise KeyError

    def __setitem__(self, key, value):
        if key in self.__attrs:
            attrValue = self.__attrs[key]
            if attrValue.repeating:
                if value is None:
                    attrValue.values = []
                elif isinstance(value, list):
                    attrValue.values = value
                else:
                    attrValue.values = [value]
            else:
                if value is None:
                    attrValue.values = []
                elif isinstance(value, list):
                    if len(value) > 1:
                        raise RuntimeError("Single attribute %s does not accept arrays" % key)
                    elif len(value) == 0:
                        attrValue.values = []
                    else:
                        val = value[0]
                        if val is None:
                            attrValue.values = []
                        else:
                            attrValue.values = [val]
        else:
            raise KeyError

    def __iter__(self):
        return iter(self.__attrs.keys())
