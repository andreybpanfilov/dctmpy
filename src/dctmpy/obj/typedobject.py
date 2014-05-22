#  Copyright (c) 2013 Andrey B. Panfilov <andrew@panfilov.tel>
#
#  See main module for license.
#
from decimal import Decimal

from dctmpy import *


class TypedObject(object):
    attributes = ['session', 'type', 'buffer', 'serversion', 'iso8601time']

    def __init__(self, **kwargs):
        for attribute in TypedObject.attributes:
            setattr(self, attribute, kwargs.pop(attribute, None))
        self.__attrs = {}

        if self.serversion is None:
            self.serversion = self.session.serversion

        if self.iso8601time is None:
            if self.serversion == 2:
                self.iso8601time = self.session.iso8601time
            else:
                self.iso8601time = False

        if not is_empty(self.buffer):
            self._read()

    def _read(self, buf=None):
        if is_empty(buf) and is_empty(self.buffer):
            raise ParserException("Empty data")
        elif not is_empty(buf):
            self.buffer = buf

        self._read_header()

        if self.type is None and self._need_read_type():
            self.type = self._read_type()

        if self._need_read_object():
            self._read_object()

    def _read_header(self):
        if self.serversion > 0:
            serversion = self._read_int()
            if serversion != self.serversion:
                raise RuntimeError(
                    "Invalid serialization version %d, expected %d" % (serversion, self.serversion))

    def _read_type(self):
        header = self._next_token()
        if header != "TYPE":
            raise ParserException("Invalid type header: %s" % header)

        type_info = self._read_type_info()
        for i in xrange(0, self._read_int()):
            type_info.append(self._read_attr_info())

        return type_info

    def _read_object(self):
        header = self._next_token()
        if "OBJ" != header:
            raise ParserException("Invalid header, expected OBJ, got: %s" % header)

        type_name = self._next_token()

        if not type_name:
            raise ParserException("Wrong type name")

        if self.serversion > 0:
            self._read_int()
            self._read_int()
            self._read_int()

        if self.type is None or type_name != self.type.name:
            raise ParserException("No type info for %s" % type_name)

        for i in xrange(0, self._read_int()):
            self._read_attr(i)

        self._read_extended_attr()

    def _read_attr(self, index):
        position = self._if_d6(self._read_base64_int)
        if position is None:
            position = index

        repeating = self.type.get(position).repeating
        attr_type = self.type.get(position).type

        if self.serversion == 2:
            repeating = self._next_string(REPEATING_PATTERN) == REPEATING
            entry_type = self._read_int()
            if entry_type in TYPES:
                attr_type = TYPES[entry_type]

        attr_name = self.type.get(position).name
        attr_length = self.type.get(position).length

        if not attr_type:
            raise ParserException("Unknown type")

        result = []

        if not repeating:
            result.append(self._read_attr_value(attr_type))
        else:
            for i in xrange(0, self._read_int()):
                result.append(self._read_attr_value(attr_type))

        self.add(AttrValue(**{
            'name': attr_name,
            'position': position,
            'type': attr_type,
            'length': attr_length,
            'values': result,
            'repeating': repeating,
        }))

    def add(self, value):
        self.__attrs[value.name] = value

    def _read_extended_attr(self):
        attr_count = self._read_int()
        for i in xrange(0, attr_count):
            attr_name = self._next_string(ATTRIBUTE_PATTERN)
            attr_type = self._next_string(ATTRIBUTE_PATTERN)
            repeating = REPEATING == self._next_string()
            length = self._read_int()

            if is_empty(attr_type):
                raise ParserException("Unknown typedef: %s" % attr_type)

            result = []

            if not repeating:
                result.append(self._read_attr_value(attr_type))
            else:
                for i in xrange(1, self._read_int()):
                    result.append(self._read_attr_value(attr_type))

            self.__attrs[attr_name] = AttrValue(**{
                'name': attr_name,
                'type': attr_type,
                'length': length,
                'values': result,
                'repeating': repeating,
            })

    def _read_attr_value(self, attr_type):
        return {
            INT: lambda: self._read_int(),
            STRING: lambda: self._read_string(),
            TIME: lambda: self._read_time(),
            BOOL: lambda: self._read_boolean(),
            ID: lambda: self._next_string(),
            DOUBLE: lambda: self._read_double(),
            UNDEFINED: lambda: self._next_string()
        }[attr_type]()

    def _read_type_info(self):
        return TypeInfo(**{
            'name': self._next_string(ATTRIBUTE_PATTERN),
            'id': self._next_string(ATTRIBUTE_PATTERN),
            'vstamp': self._if_d6(self._read_int),
            'version': self._if_d6(self._read_int),
            'cache': self._if_d6(self._read_int),
            'super': self._next_string(ATTRIBUTE_PATTERN),
            'sharedparent': self._if_d6(self._next_string, None, ATTRIBUTE_PATTERN),
            'aspectname': self._if_d6(self._next_string, None, ATTRIBUTE_PATTERN),
            'aspectshareflag': self._if_d6(self._read_boolean),
            'serversion': self.serversion,
        })

    def _read_attr_info(self):
        return AttrInfo(**{
            'position': self._if_d6(self._read_base64_int),
            'name': self._next_string(ATTRIBUTE_PATTERN),
            'type': self._next_string(TYPE_PATTERN),
            'repeating': REPEATING == self._next_string(),
            'length': self._read_int(),
            'restriction': self._if_d6(self._read_int),
        })

    def _if_d6(self, method, default=None, *args, **kwargs):
        if self.serversion > 0:
            return method(*args, **kwargs)
        return default

    def serialize(self):
        result = ""
        if self.serversion > 0:
            result += "%d\n" % self.serversion
        result += "OBJ NULL 0 "
        if self.serversion > 0:
            result += "0 0\n0\n"
        result += "%d\n" % len(self.__attrs)
        for attr_value in self.__attrs.values():
            result += "%s %s %s %d\n" % (
                attr_value.name, attr_value.type, [SINGLE, REPEATING][attr_value.repeating],
                attr_value.length)
            if attr_value.repeating:
                result += "%d\n" % len(attr_value.values)
            for value in attr_value.values:
                if STRING == attr_value.type:
                    result += "A %d %s\n" % (len(value), value)
                elif BOOL == attr_value.type:
                    result += "%s\n" % ["F", "T"][value]
                else:
                    result += "%s\n" % value
        return result

    def _need_read_type(self):
        return True

    def _need_read_object(self):
        return True

    def _substr(self, length):
        data = self.buffer
        self.buffer = data[length:]
        return data[:length]

    def _next_token(self, separator=DEFAULT_SEPARATOR):
        m = re.search(r'^%s' % separator, self.buffer)
        if m:
            self._substr(m.end(0))
        m = re.search(separator, self.buffer)
        if m:
            return self._substr(m.start(0))
        else:
            return self._substr(len(self.buffer))

    def _next_string(self, pattern=None, separator=DEFAULT_SEPARATOR):
        value = self._next_token(separator)
        if pattern:
            if not re.match(pattern, value):
                raise ParserException("Invalid string: %s for regexp %s" % (value, pattern))
        return value

    def _read_int(self):
        return int(self._next_string(INTEGER_PATTERN))

    def _read_base64_int(self):
        return pseudo_base64_to_int(self._next_string(BASE64_PATTERN))

    def _read_string(self):
        self._next_string(ENCODING_PATTERN)
        return self._substr(self._read_int() + 1)[1:]

    def _read_time(self):
        value = self._next_token(CRLF_PATTERN)
        if value.startswith(" "):
            value = value[1:]
        if value.startswith("xxx "):
            value = value[4:]
        return parse_time(value)

    def _read_boolean(self):
        value = self._next_string(BOOLEAN_PATTERN)
        return value == 'T' or value == '1'

    def _read_double(self):
        return Decimal(self._next_string())

    def __getattr__(self, name):
        if name in self.__attrs:
            return self.__attrs[name]
        elif name in TypedObject.attributes:
            return self.__getattribute__(ATTRIBUTE_PREFIX + name)
        else:
            raise AttributeError("Unknown attribute %s in %s" % (name, str(self.__class__)))

    def __setattr__(self, name, value):
        if name in TypedObject.attributes:
            TypedObject.__setattr__(self, ATTRIBUTE_PREFIX + name, value)
        else:
            super(TypedObject, self).__setattr__(name, value)

    def __len__(self):
        return len(self.__attrs)

    def __contains__(self, key):
        return key in self.__attrs

    def __getitem__(self, key):
        if key in self.__attrs:
            attr_value = self.__attrs[key]
            if attr_value.repeating:
                return attr_value.values
            else:
                return attr_value[0]
        else:
            raise KeyError("invalid key \"%s\"" % key)

    def __setitem__(self, key, value):
        if key in self.__attrs:
            attr_value = self.__attrs[key]
            if attr_value.repeating:
                if value is None:
                    attr_value.values = []
                elif isinstance(value, list):
                    attr_value.values = value
                else:
                    attr_value.values = [value]
            else:
                if value is None:
                    attr_value.values = []
                elif isinstance(value, list):
                    if len(value) > 1:
                        raise RuntimeError("Single attribute %s does not accept arrays" % key)
                    elif len(value) == 0:
                        attr_value.values = []
                    else:
                        val = value[0]
                        if val is None:
                            attr_value.values = []
                        else:
                            attr_value.values = [val]
        else:
            raise KeyError

    def __iter__(self):
        return iter(self.__attrs.keys())
