#  Copyright (c) 2013 Andrey B. Panfilov <andrew@panfilov.tel>
#
#  See main module for license.
#

from dctmpy import *


class TypedObject(object):
    attributes = ['session', 'type', 'buffer', 'serializationversion', 'iso8601time']

    def __init__(self, **kwargs):
        for attribute in TypedObject.attributes:
            self.__setattr__(ATTRIBUTE_PREFIX + attribute, kwargs.pop(attribute, None))
        self.__attrs = {}

        if self.serializationversion is None:
            self.serializationversion = self.session.serializationversion

        if self.iso8601time is None:
            if self.serializationversion == 2:
                self.iso8601time = self.session.iso8601time
            else:
                self.iso8601time = False

        if not isempty(self.buffer):
            self.__read__()

    def __read__(self, buf=None):
        if isempty(buf) and isempty(self.buffer):
            raise ParserException("Empty data")
        elif not isempty(buf):
            self.buffer = buf

        self.__read_header__()

        if self.type is None and self.__need_read_type__():
            self.type = self.__read_type__()

        if self.__need_read_object__():
            self.__read_object__()

    def __read_header__(self):
        if self.serializationversion > 0:
            serializationversion = self.__read_int__()
            if serializationversion != self.serializationversion:
                raise RuntimeError(
                    "Invalid serialization version %d, expected %d" % (serializationversion, self.serializationversion))

    def __read_type__(self):
        header = self.__next_token__()
        if header != "TYPE":
            raise ParserException("Invalid type header: %s" % header)

        type_info = self.__read_type_info__()
        for i in xrange(0, self.__read_int__()):
            type_info.append(self.__read_attr_info__())

        return type_info

    def __read_object__(self):
        header = self.__next_token__()
        if "OBJ" != header:
            raise ParserException("Invalid header, expected OBJ, got: %s" % header)

        type_name = self.__next_token__()

        if type_name is None or len(type_name) == 0:
            raise ParserException("Wrong type name")

        if self.serializationversion > 0:
            self.__read_int__()
            self.__read_int__()
            self.__read_int__()

        if self.type is None or type_name != self.type.name:
            raise ParserException("No type info for %s" % type_name)

        for i in xrange(0, self.__read_int__()):
            self.__read_attr__(i)

        self.__read_extended_attr__()

    def __read_attr__(self, index):
        position = self.__if_d6(self.__read_base64_int__)
        if position is None:
            position = index

        repeating = self.type.get(position).repeating
        attr_type = self.type.get(position).type

        if self.serializationversion == 2:
            repeating = self.__next_string__(REPEATING_PATTERN) == REPEATING
            attr_type = TYPES[self.__read_int__()]

        attr_name = self.type.get(position).name
        attr_length = self.type.get(position).length

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
            'position': position,
            'type': attr_type,
            'length': attr_length,
            'values': result,
            'repeating': repeating,
        }))

    def add(self, value):
        self.__attrs[value.name] = value

    def __read_extended_attr__(self):
        attr_count = self.__read_int__()
        for i in xrange(0, attr_count):
            attr_name = self.__next_string__(ATTRIBUTE_PATTERN)
            attr_type = self.__next_string__(ATTRIBUTE_PATTERN)
            repeating = REPEATING == self.__next_string__()
            length = self.__read_int__()

            if isempty(attr_type):
                raise ParserException("Unknown typedef: %s" % attr_type)

            result = []

            if not repeating:
                result.append(self.__read_attr_value__(attr_type))
            else:
                for i in xrange(1, self.__read_int__()):
                    result.append(self.__read_attr_value__(attr_type))

            self.__attrs[attr_name] = AttrValue(**{
                'name': attr_name,
                'type': attr_type,
                'length': length,
                'values': result,
                'repeating': repeating,
            })

    def __read_attr_value__(self, attr_type):
        return {
            INT: lambda: self.__read_int__(),
            STRING: lambda: self.__read_string__(),
            TIME: lambda: self.__read_time__(),
            BOOL: lambda: self.__read_boolean__(),
            ID: lambda: self.__next_string__(),
            DOUBLE: lambda: self.__next_string__(),
            UNDEFINED: lambda: self.__next_string__()
        }[attr_type]()

    def __read_type_info__(self):
        return TypeInfo(**{
            'name': self.__next_string__(ATTRIBUTE_PATTERN),
            'id': self.__next_string__(ATTRIBUTE_PATTERN),
            'vstamp': self.__if_d6(self.__read_int__),
            'version': self.__if_d6(self.__read_int__),
            'cache': self.__if_d6(self.__read_int__),
            'super': self.__next_string__(ATTRIBUTE_PATTERN),
            'sharedparent': self.__if_d6(self.__next_string__, None, ATTRIBUTE_PATTERN),
            'aspectname': self.__if_d6(self.__next_string__, None, ATTRIBUTE_PATTERN),
            'aspectshareflag': self.__if_d6(self.__read_boolean__),
            'serializationversion': self.serializationversion,
        })

    def __read_attr_info__(self):
        return AttrInfo(**{
            'position': self.__if_d6(self.__read_base64_int__),
            'name': self.__next_string__(ATTRIBUTE_PATTERN),
            'type': self.__next_string__(TYPE_PATTERN),
            'repeating': REPEATING == self.__next_string__(),
            'length': self.__read_int__(),
            'restriction': self.__if_d6(self.__read_int__),
        })

    def __if_d6(self, method, default=None, *args, **kwargs):
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

    def __need_read_type__(self):
        return True

    def __need_read_object__(self):
        return True

    def __substr__(self, length):
        data = self.buffer
        self.buffer = data[length:]
        return data[:length]

    def __next_token__(self, separator=DEFAULT_SEPARATOR):
        self.buffer = re.sub("^%s" % separator, "", self.buffer)
        m = re.search(separator, self.buffer)
        if m is not None:
            return self.__substr__(m.start(0))
        else:
            return self.__substr__(len(self.buffer))

    def __next_string__(self, pattern=None, separator=DEFAULT_SEPARATOR):
        value = self.__next_token__(separator)
        if pattern is not None:
            if re.match(pattern, value) is None:
                raise ParserException("Invalid string: %s for regexp %s" % (value, pattern))
        return value

    def __read_int__(self):
        return int(self.__next_string__(INTEGER_PATTERN))

    def __read_base64_int__(self):
        return pseudo_base64_to_int(self.__next_string__(BASE64_PATTERN))

    def __read_string__(self):
        self.__next_string__(ENCODING_PATTERN)
        return self.__substr__(self.__read_int__() + 1)[1:]

    def __read_time__(self):
        value = self.__next_token__(CRLF_PATTERN)
        if value.startswith(" "):
            value = value[1:]
        if value.startswith("xxx "):
            value = value[4:]
        return parse_time(value, self.iso8601time)

    def __read_boolean__(self):
        return bool(self.__next_string__(BOOLEAN_PATTERN))

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
            raise KeyError

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
