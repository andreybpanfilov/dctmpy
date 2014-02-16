#  Copyright (c) 2013 Andrey B. Panfilov <andrew@panfilov.tel>
#
#  See main module for license.
#

import array
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


def serializeInteger(value):
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


def serializeLength(value):
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


def serializeString(value):
    result = array.array('B')
    if value is None or len(value) == 0:
        result.append(EMPTY_STRING_START)
        result.append(NULL_BYTE)
        return result
    result.extend(stringToIntegerArray(value))
    result.append(NULL_BYTE)
    for b in serializeLength(len(result))[::-1]:
        result.insert(0, b)
    result.insert(0, STRING_START)
    return result


def serializeId(value):
    if value is None or len(value) == 0:
        value = NULL_ID
    return serializeString(value)


def serializeIntegerArray(intarray):
    result = []
    for i in intarray:
        result.extend(serializeInteger(i))
    result.insert(0, len(result) & 0x000000ff)
    result.insert(0, (len(result) - 1) >> 8)
    result.insert(0, LONG_LENGTH_START)
    result.insert(0, INT_ARRAY_START)
    return result


def serializeValue(value):
    if value is None:
        return serializeString("")
    elif isinstance(value, str):
        return serializeString(value)
    elif isinstance(value, int):
        return serializeInteger(value)
    elif isinstance(value, list):
        return serializeIntegerArray(value)
    elif isinstance(value, TypedObject):
        return serializeString(value.serialize())
    elif hasattr(value.__class__, "serialize"):
        return serializeString(value.serialize())
    else:
        raise TypeError("Invalid argument type")


def serializeData(data=None):
    result = array.array('B')
    if data is None:
        return result
    for i in data:
        result.extend(serializeValue(i))
    return result


def readInteger(data):
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


def readLength(data):
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


def readIntegerArray(data):
    header = data.pop(0)
    if header != INT_ARRAY_START:
        raise RuntimeError("Wrong sequence for integer array: 0x%X" % header)

    length = readLength(data)
    stop = len(data) - length
    result = []
    while len(data) > stop:
        result.append(readInteger(data))
    return result


def readArray(data, asstring=False):
    sequence = data.pop(0)
    if sequence == EMPTY_STRING_START and data.pop(0) == NULL_BYTE:
        return []
    elif sequence == STRING_START:
        length = readLength(data)
        result = []
        for i in xrange(0, length):
            result.append(data.pop(0))
        if asstring and result[len(result) - 1] == NULL_BYTE:
            result.pop()
        return result
    elif sequence == STRING_ARRAY_START and data.pop(0) == 0x80:
        result = []
        while data[0] != NULL_BYTE or data[1] != NULL_BYTE:
            result.append(readArray(data, asstring))
        data.pop(0)
        data.pop(0)
        return result
    raise RuntimeError("Unknown sequence: 0x%X" % sequence)


def readString(data):
    return integerArrayToString(readArray(data, True))


def readStrings(data):
    result = []
    for res in readArray(data):
        result.append(integerArrayToString(res))
    return result

