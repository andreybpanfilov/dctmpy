#  Copyright (c) 2013 Andrey B. Panfilov <andrew@panfilov.tel>
#
#  See main module for license.
#

from dctmpy import *
from dctmpy.obj.typedobject import TypedObject


class TypeObject(TypedObject):
    def __init__(self, **kwargs):
        self.__typeCont = None
        super(TypeObject, self).__init__(**kwargs)

    def read(self, buf=None):
        self.readHeader()
        typeInfo = None
        if self.__typeCont is not None:
            for i in xrange(0, self.__typeCont):
                typeInfo = self.deserializeChildType()
        else:
            while not isEmpty(self.buffer()):
                typeInfo = self.deserializeChildType()
        self.type = typeInfo

    def deserializeChildType(self):
        childType = self.readType()
        if childType is not None:
            addTypeToCache(childType)
        return childType

    def readType(self):
        return super(TypeObject, self).readType()

    def readHeader(self):
        self.__typeCont = self.readInt()
        if self.serializationversion > 0:
            self.readInt()

    def needReadType(self):
        return False

    def needReadObject(self):
        return False
