# Copyright (c) 2013 Andrey B. Panfilov <andrew@panfilov.tel>
#
# See main module for license.
#
from decimal import Decimal

from dctmpy import *
from dctmpy.exceptions import ParserException
from dctmpy.obj.attrinfo import AttrInfo
from dctmpy.obj.attrvalue import AttrValue
from dctmpy.obj.typeinfo import TypeInfo


class TypedObject(object):
    attributes = ['session', 'type', 'buffer', 'initial', 'ser_version', 'iso8601time', 'attrs']

    def __init__(self, **kwargs):
        for attribute in TypedObject.attributes:
            setattr(self, attribute, kwargs.pop(attribute, None))

        if self.attrs is None:
            self.attrs = {}

        if self.ser_version is None:
            self.ser_version = self.session.ser_version

        if self.iso8601time is None:
            if self.ser_version == 2:
                self.iso8601time = self.session.iso8601time
            else:
                self.iso8601time = False

        if not self._is_empty():
            self._read()

    def _is_empty(self):
        return is_empty(self.buffer)

    def _read(self, buf=None):
        if is_empty(buf) and is_empty(self.buffer):
            raise ParserException("Empty data")
        elif not is_empty(buf):
            self.buffer = buf
        self.initial = self.buffer
        self._read_header()
        if self.type is None and self._need_read_type():
            self.type = self._read_type()
        if self._need_read_object():
            self._read_object()
        self.initial = None

    def _read_header(self):
        if self.ser_version > 0:
            ser_version = self._read_int()
            if ser_version != self.ser_version:
                raise RuntimeError(
                    "Invalid serialization version %d, expected %d" % (ser_version, self.ser_version))

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

        if self.ser_version > 0:
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

        if self.ser_version == 2:
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
            'extended': False,
        }))

    def add(self, value):
        self.attrs[value.name] = value

    def _set(self, name, type, value):
        existing = self.attrs.get(name, None)
        if existing is None:
            existing = AttrValue(**{
                'name': name,
                'type': type,
                'repeating': False
            })
        existing.values = [value]
        self.add(existing)

    def set_string(self, name, value):
        self._set(name, STRING, value)

    def set_id(self, name, value):
        self._set(name, ID, value)

    def set_int(self, name, value):
        self._set(name, INT, value)

    def set_bool(self, name, value):
        self._set(name, BOOL, value)

    def set_double(self, name, value):
        self._set(name, DOUBLE, value)

    def set_time(self, name, value):
        self._set(name, TIME, value)

    def _append(self, name, type, value):
        values = as_list(value)
        existing = self.attrs.get(name, None)
        if existing is None:
            existing = AttrValue(**{
                'name': name,
                'type': type,
                'repeating': True
            })
        if existing.values is None:
            existing.values = values
        else:
            existing.values += values
        self.add(existing)

    def append_string(self, name, value):
        self._append(name, STRING, value)

    def append_id(self, name, value):
        self._append(name, ID, value)

    def append_int(self, name, value):
        self._append(name, INT, value)

    def append_bool(self, name, value):
        self._append(name, BOOL, value)

    def append_double(self, name, value):
        self._append(name, DOUBLE, value)

    def append_time(self, name, value):
        self._append(name, TIME, value)

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
                for i in xrange(0, self._read_int()):
                    result.append(self._read_attr_value(attr_type))

            self.attrs[attr_name] = AttrValue(**{
                'name': attr_name,
                'type': attr_type,
                'length': length,
                'values': result,
                'repeating': repeating,
                'extended': True,
            })

    def _read_attr_value(self, attr_type):
        return {
            INT: TypedObject._read_int,
            STRING: TypedObject._read_string,
            TIME: TypedObject._read_time,
            BOOL: TypedObject._read_boolean,
            ID: TypedObject._next_string,
            DOUBLE: TypedObject._read_double,
            UNDEFINED: TypedObject._next_string
        }[attr_type](self)

    def _read_type_info(self):
        return TypeInfo(**{
            'name': self._next_string(ATTRIBUTE_PATTERN),
            'id': self._next_string(ATTRIBUTE_PATTERN),
            'vstamp': self._if_d6(self._read_int),
            'version': self._if_d6(self._read_int),
            'cache': self._if_d6(self._read_int),
            'super': self._next_string(ATTRIBUTE_PATTERN),
            'shared_parent': self._if_d6(self._next_string, None, ATTRIBUTE_PATTERN),
            'aspect_name': self._if_d6(self._next_string, None, ATTRIBUTE_PATTERN),
            'aspect_share_flag': self._if_d6(self._read_boolean),
            'ser_version': self.ser_version,
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
        if self.ser_version > 0:
            return method(*args, **kwargs)
        return default

    def serialize(self):
        result = ""
        if self.ser_version > 0:
            result += "%d\n" % self.ser_version
        result += "OBJ NULL 0 "
        if self.ser_version > 0:
            result += "0 0\n0\n"
        result += "%d\n" % len(self.attrs)
        for attr_value in self.attrs.values():
            result += "%s %s %s %d\n" % (
                attr_value.name, attr_value.type, [SINGLE, REPEATING][attr_value.repeating],
                attr_value.length)
            if attr_value.repeating:
                result += "%d\n" % len(attr_value.values)
            for value in attr_value.values:
                if STRING == attr_value.type:
                    if value is None:
                        value = ""
                    result += "A %d %s\n" % (len(value), value)
                elif ID == attr_value.type:
                    if is_empty(value):
                        value = NULL_ID
                    result += "%s\n" % value
                elif BOOL == attr_value.type:
                    if value is None:
                        value = False
                    result += "%s\n" % ["F", "T"][value]
                elif INT == attr_value.type or DOUBLE == attr_value.type:
                    if value is None:
                        value = 0
                    result += "%s\n" % value
                elif TIME == attr_value.type:
                    if value is None:
                        value = "nulldate"
                    result += "%s\n" % value
                else:
                    result += "%s\n" % value
        return result

    def _need_read_type(self):
        return True

    def _need_read_object(self):
        return True

    def _substr(self, length, trim=-1):
        result = str(buffer(self.buffer, 0, length))
        for c in buffer(self.buffer, length):
            if trim == 0 or not c.isspace():
                break
            length += 1
            trim -= 1
        self.buffer = buffer(self.buffer, length)
        return result

    def _next_token(self, trim=-1):
        length = 0
        for c in self.buffer:
            if c.isspace():
                break
            length += 1
        return self._substr(length, trim)

    def _next_string(self, pattern=None, trim=-1):
        value = self._next_token(trim)
        if pattern:
            if not pattern.match(value):
                raise ParserException("Invalid string: %s for regexp %s" % (value, pattern))
        return value

    def _read_int(self, trim=-1):
        return int(self._next_string(INTEGER_PATTERN, trim))

    def _read_base64_int(self, trim=-1):
        return pseudo_base64_to_int(self._next_string(BASE64_PATTERN, trim))

    def _read_string(self):
        encoding = self._next_string(ENCODING_PATTERN)
        length = self._read_int(1)
        if encoding == 'H':
            length *= 2
        result = self._substr(length)
        if encoding == 'H':
            return result.decode("hex")
        return result

    def _read_time(self):
        value = self._next_token()
        if value == "xxx":
            value = self._substr(20)
        return parse_time(value)

    def _read_boolean(self):
        value = self._next_string(BOOLEAN_PATTERN)
        return value == 'T' or value == '1'

    def _read_double(self):
        return Decimal(self._next_string())

    def __len__(self):
        return len(self.attrs)

    def __contains__(self, key):
        return key in self.attrs

    def __getitem__(self, key):
        if key in self.attrs:
            attr_value = self.attrs[key]
            if attr_value.repeating:
                return attr_value.values
            else:
                return attr_value[0]
        else:
            raise KeyError("invalid key \"%s\"" % key)

    def __setitem__(self, key, value):
        if key in self.attrs:
            attr_value = self.attrs[key]
            values = as_list(value)
            if attr_value.repeating and len(values) > 1:
                raise RuntimeError("Single attribute %s does not accept arrays" % key)
            attr_value.values = values
        else:
            raise KeyError

    def __iter__(self):
        return iter(self.attrs.keys())

    def dump(self):
        primary = ""
        extended = ""
        fmt = "\n %-30s %8s: %s"
        for attr in self.attrs:
            value = self.attrs[attr]
            data = ""
            if not value.repeating:
                data += (fmt % (attr, "", value.values[0]))
            else:
                count = len(value.values)
                if count == 0:
                    data += fmt % (attr, "[]", "<none>")
                else:
                    for i in xrange(0, count):
                        if i == 0:
                            data += (fmt % (attr, "[" + str(i) + "]", value.values[i]))
                        else:
                            data += (fmt % ("", "[" + str(i) + "]", value.values[i]))
            if value.extended:
                extended += data
            else:
                primary += data
        if len(extended) == 0:
            return "ATTRIBUTES:%s" % primary
        return "ATTRIBUTES:%s\nEXTENDED:%s" % (primary, extended)
