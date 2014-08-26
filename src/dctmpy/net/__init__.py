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
    while value > 127 or value < -128:
        result.append(value & 0x000000ff)
        value >>= 8
    result.append(value & 0xFF)
    result.append(len(result))
    result.append(INTEGER_START)
    result.reverse()
    return result


def serialize_integer(value):
    if value is None:
        raise RuntimeError("Undefined integer value")
    if -65535 <= value < 65536:
        if not value in INTEGERS:
            INTEGERS[value] = _serialize_integer(value)
        return INTEGERS[value]
    return _serialize_integer(value)


def _serialize_length(value):
    if value is None:
        raise RuntimeError("Undefined integer value")
    result = bytearray()
    while value > 127:
        result.append(value & 0x000000ff)
        value >>= 8
    result.append(value)
    if len(result) > 1:
        result.append(len(result) | 0x80)
    result.reverse()
    return result


def serialize_length(value):
    if value is None:
        raise RuntimeError("Undefined integer value")
    if value < 65536:
        if not value in LENGTHS:
            LENGTHS[value] = _serialize_length(value)
        return LENGTHS[value]
    return _serialize_length(value)


def serialize_string(value):
    result = bytearray()
    if not value:
        result.append(EMPTY_STRING_START)
        result.append(NULL_BYTE)
        return result
    result.extend(value)
    result.append(NULL_BYTE)
    for b in reversed(serialize_length(len(result))):
        result.insert(0, b)
    result.insert(0, STRING_START)
    return result


def serialize_id(value):
    if not value:
        value = NULL_ID
    return serialize_string(value)


def serialize_integer_array(intarray):
    result = bytearray(4)
    for i in intarray:
        result.extend(serialize_integer(i))
    result[3] = (len(result) - 4) & 0x000000ff
    result[2] = (len(result) - 4) >> 8
    result[1] = LONG_LENGTH_START
    result[0] = INT_ARRAY_START
    return result


def serialize_value(value):
    if value is None:
        return serialize_string("")
    elif isinstance(value, str):
        return serialize_string(value)
    elif isinstance(value, bytearray):
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
    result = bytearray()
    if data is None:
        return result
    for i in data:
        result.extend(serialize_value(i))
    return result


def read_integer(data):
    if len(data) < 3:
        raise RuntimeError("Wrong sequence, at least 3 bytes required, got: %d" % len(data))

    (header, length, value) = data[-1:-4:-1]
    if header != INTEGER_START:
        raise RuntimeError("Wrong sequence for integer: 0x%X" % header)

    if len(data) < length + 2:
        raise RuntimeError("Wrong sequence, at least %d bytes required, got: %d" % (length + 2, len(data)))

    value -= (value & 0x80) << 1
    for i in xrange(1, length):
        value = value << 8 | data[-3 - i]
        if value > 0x7fffffff:
            value = value - 0xffffffff - 1
    del data[-length - 2:]
    return value


def read_length(data):
    if len(data) < 1:
        raise RuntimeError("Wrong sequence, at least 1 byte required, got: %d" % len(data))

    value = data.pop()

    if value <= 127:
        return value

    length = value & 0x7F

    if len(data) < length:
        raise RuntimeError("Wrong sequence, %d bytes required, got: %d" % (length, len(data)))

    value = data[-1]
    for i in xrange(1, length):
        value = value << 8 | data[-1 - i]
        if value > 0x7fffffff:
            value = value - 0xffffffff - 1
    del data[-length:]
    return value


def read_integer_array(data):
    header = data.pop()
    if header != INT_ARRAY_START:
        raise RuntimeError("Wrong sequence for integer array: 0x%X" % header)

    length = read_length(data)
    stop = len(data) - length
    result = []
    while len(data) > stop:
        result.append(read_integer(data))
    return result


def read_array(data, asstring=False):
    sequence = data[-1]
    if sequence == EMPTY_STRING_START and data[-2] == NULL_BYTE:
        del data[-2:]
        return bytearray()
    elif sequence == STRING_START:
        del data[-1:]
        length = read_length(data)
        if asstring and data[-length] == NULL_BYTE:
            result = data[-length + 1:]
        else:
            result = data[-length:]
        del data[-length:]
        result.reverse()
        return result
    elif sequence == STRING_ARRAY_START and data[-2] == 0x80:
        del data[-2:]
        result = bytearray()
        while data[-1] != NULL_BYTE or data[-2] != NULL_BYTE:
            result.extend(read_array(data, asstring))
        del data[-2:]
        return result
    raise RuntimeError("Unknown sequence: 0x%X" % sequence)


def read_string(data):
    return read_array(data, True)


def read_binary(data):
    return read_array(data, False)


