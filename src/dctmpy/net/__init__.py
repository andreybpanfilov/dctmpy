# Copyright (c) 2013 Andrey B. Panfilov <andrew@panfilov.tel>
#
# See main module for license.
#

from dctmpy import *
from dctmpy.obj.typedobject import TypedObject

PROTOCOL_VERSION = 0x30
INTEGER_START = 0x02
EMPTY_STRING_START = 0x05
NULL_BYTE = 0x00
STRING_START = 0x16
INT_ARRAY_START = 0x30
LONG_LENGTH_START = 0x82
STRING_ARRAY_START = 0x36

INTEGERS = {}
LENGTHS = {}


def _serialize_integer(value):
    if value is None:
        raise RuntimeError("Undefined integer value")
    result = bytearray()
    while value >= 0x80 or value < -0x80:
        result.append(value & 0xff)
        value >>= 8
    result.append(value & 0xff)
    result.append(len(result))
    result.append(INTEGER_START)
    result.reverse()
    return result


def serialize_integer(value):
    if value is None:
        raise RuntimeError("Undefined integer value")
    if -0xffff <= value <= 0xffff:
        if value not in INTEGERS:
            INTEGERS[value] = _serialize_integer(value)
        return INTEGERS[value]
    return _serialize_integer(value)


def _serialize_length(value):
    if value is None:
        raise RuntimeError("Undefined integer value")
    result = bytearray()
    while value >= 0x80:
        result.append(value & 0xff)
        value >>= 8
    result.append(value)
    if len(result) > 1:
        result.append(len(result) | 0x80)
    result.reverse()
    return result


def serialize_length(value):
    if value is None:
        raise RuntimeError("Undefined integer value")
    if value <= 0xffff:
        if value not in LENGTHS:
            LENGTHS[value] = _serialize_length(value)
        return LENGTHS[value]
    return _serialize_length(value)


def serialize_array(value, asstring=False):
    result = bytearray()
    if not value:
        result.append(EMPTY_STRING_START)
        result.append(NULL_BYTE)
        return result
    result.extend(value)
    if asstring:
        result.append(NULL_BYTE)
    for b in reversed(serialize_length(len(result))):
        result.insert(0, b)
    result.insert(0, STRING_START)
    return result


def serialize_string(value):
    return serialize_array(value, True)


def serialize_id(value):
    if not value:
        value = NULL_ID
    return serialize_string(value)


def serialize_integer_array(intarray):
    result = bytearray(4)
    for i in intarray:
        result.extend(serialize_integer(i))
    result[3] = (len(result) - 4) & 0xff
    result[2] = (len(result) - 4) >> 8
    result[1] = LONG_LENGTH_START
    result[0] = INT_ARRAY_START
    return result


def serialize_value(value):
    if value is None:
        return serialize_string("")
    elif isinstance(value, str):
        return serialize_string(value)
    elif isinstance(value, buffer):
        return serialize_string(value)
    elif isinstance(value, bytearray):
        return serialize_array(value)
    elif isinstance(value, int):
        return serialize_integer(value)
    elif isinstance(value, list):
        return serialize_integer_array(value)
    elif isinstance(value, TypedObject):
        return serialize_string(value.serialize())
    elif hasattr(value.__class__, "serialize"):
        return serialize_string(value.serialize())
    else:
        raise TypeError("Invalid argument type")


def serialize_data(data=None):
    result = bytearray()
    if data is None:
        return result
    for i in data:
        result.extend(serialize_value(i))
    return result


def read_integer(data, offset=0):
    (header, length, value) = (data[offset], data[offset + 1], data[offset + 2])
    if header != INTEGER_START:
        raise RuntimeError("Wrong sequence for integer: 0x%X" % header)
    if value > 0x7f:
        value = ~value ^ 0xff
    if length > 1:
        value = value << 8 | data[3 + offset]
    if length > 2:
        value = value << 8 | data[4 + offset]
    if length > 3:
        value = value << 8 | data[5 + offset]
    if value > 0x7fffffff:
        value = ~value ^ 0xffffffff
    return value, offset + 2 + length


def read_length(data, offset=0):
    value = data[offset]
    offset += 1
    if value < 0x80:
        return value, offset
    length = value & 0x7f
    value = data[offset]
    if length > 1:
        value = value << 8 | data[1 + offset]
    if length > 2:
        value = value << 8 | data[2 + offset]
    if length > 3:
        value = value << 8 | data[3 + offset]
    return value, offset + length


def read_integer_array(data, offset=0):
    header = data[offset]
    offset += 1
    if header != INT_ARRAY_START:
        raise RuntimeError("Wrong sequence for integer array: 0x%X" % header)

    (length, offset) = read_length(data, offset)
    stop = offset + length
    result = []
    while stop > offset:
        (chunk, offset) = read_integer(data, offset)
        result.append(chunk)
    return result, offset


def read_array(data, offset=0, asstring=False):
    sequence = data[offset]
    if sequence == EMPTY_STRING_START and data[1 + offset] == NULL_BYTE:
        return bytearray(), offset + 2
    elif sequence == STRING_START:
        (length, offset) = read_length(data, offset + 1)
        if asstring and data[offset + length - 1] == NULL_BYTE:
            result = data[offset: offset + length - 1]
        else:
            result = data[offset: offset + length]
        return result, offset + length
    elif sequence == STRING_ARRAY_START and data[1 + offset] == 0x80:
        offset += 2
        result = bytearray()
        while data[offset] != NULL_BYTE or data[offset + 1] != NULL_BYTE:
            (chunk, offset) = read_array(data, offset, asstring)
            result.extend(chunk)
        return result, offset + 2
    raise RuntimeError("Unknown sequence: 0x%X" % sequence)


def read_string(data, offset=0):
    return read_array(data, offset, True)


def read_binary(data, offset=0):
    return read_array(data, offset, False)
