#  Copyright (c) 2013 Andrey B. Panfilov <andrew@panfilov.tel>
#
#  See main module for license.
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


def serialize_integer(value):
    if value is None:
        raise RuntimeError("Undefined integer value")
    result = array.array('B')
    while value > 127 or value < -128:
        result.insert(0, value & 0x000000ff)
        value >>= 8
    result.insert(0, value & 0xFF)
    result.insert(0, len(result))
    result.insert(0, INTEGER_START)
    return result


def serialize_length(value):
    if value is None:
        raise RuntimeError("Undefined integer value")
    result = array.array('B')
    while value > 127:
        result.insert(0, value & 0x000000ff)
        value >>= 8
    result.insert(0, value)
    if len(result) > 1:
        result.insert(0, len(result) | 0x80)
    return result


def serialize_string(value):
    result = array.array('B')
    if not value:
        result.append(EMPTY_STRING_START)
        result.append(NULL_BYTE)
        return result
    result.extend(string_to_integer_array(value))
    result.append(NULL_BYTE)
    for b in serialize_length(len(result))[::-1]:
        result.insert(0, b)
    result.insert(0, STRING_START)
    return result


def serialize_id(value):
    if not value:
        value = NULL_ID
    return serialize_string(value)


def serialize_integer_array(intarray):
    result = []
    for i in intarray:
        result.extend(serialize_integer(i))
    result.insert(0, len(result) & 0x000000ff)
    result.insert(0, (len(result) - 1) >> 8)
    result.insert(0, LONG_LENGTH_START)
    result.insert(0, INT_ARRAY_START)
    return result


def serialize_value(value):
    if value is None:
        return serialize_string("")
    elif isinstance(value, str):
        return serialize_string(value)
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
    result = array.array('B')
    if data is None:
        return result
    for i in data:
        result.extend(serialize_value(i))
    return result


def read_integer(data):
    if len(data) < 3:
        raise RuntimeError("Wrong sequence, at least 3 bytes required, got: %d" % len(data))

    header = data.pop(0)
    if header != INTEGER_START:
        raise RuntimeError("Wrong sequence for integer: 0x%X" % header)

    length = data.pop(0)
    if len(data) < length:
        raise RuntimeError("Wrong sequence, at least %d bytes required, got: %d" % (length, len(data)))

    value = data.pop(0)
    value -= (value & 0x80) << 1
    for i in xrange(1, length):
        value = value << 8 | data.pop(0)
        if value > 0x7fffffff:
            value = value - 0xffffffff - 1
    return value


def read_length(data):
    if len(data) < 1:
        raise RuntimeError("Wrong sequence, at least 1 byte required, got: %d" % len(data))

    value = data.pop(0)

    if value <= 127:
        return value

    length = value & 0x7F

    if len(data) < length:
        raise RuntimeError("Wrong sequence, %d bytes required, got: %d" % (length, len(data)))

    value = data.pop(0)
    for i in xrange(1, length):
        value = value << 8 | data.pop(0)
        if value > 0x7fffffff:
            value = value - 0xffffffff - 1
    return value


def read_integer_array(data):
    header = data.pop(0)
    if header != INT_ARRAY_START:
        raise RuntimeError("Wrong sequence for integer array: 0x%X" % header)

    length = read_length(data)
    stop = len(data) - length
    result = []
    while len(data) > stop:
        result.append(read_integer(data))
    return result


def read_array(data, asstring=False):
    sequence = data.pop(0)
    if sequence == EMPTY_STRING_START and data.pop(0) == NULL_BYTE:
        return []
    elif sequence == STRING_START:
        length = read_length(data)
        result = []
        for i in xrange(0, length):
            result.append(data.pop(0))
        if asstring and result[len(result) - 1] == NULL_BYTE:
            result.pop()
        return result
    elif sequence == STRING_ARRAY_START and data.pop(0) == 0x80:
        result = []
        while data[0] != NULL_BYTE or data[1] != NULL_BYTE:
            result.append(read_array(data, asstring))
        data.pop(0)
        data.pop(0)
        return result
    raise RuntimeError("Unknown sequence: 0x%X" % sequence)


def read_string(data):
    return integer_array_to_string(read_array(data, True))


def read_binary(data):
    return integer_array(read_array(data, False))


def read_strings(data):
    result = []
    for res in read_array(data):
        result.append(integer_array_to_string(res))
    return result

