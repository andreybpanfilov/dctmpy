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

    def __read__(self, buf=None):
        self.__read_header__()
        type_info = None
        if self.__typeCont is not None:
            for i in xrange(0, self.__typeCont):
                type_info = self.__deserialize_child_type__()
        else:
            while not isempty(self.buffer()):
                type_info = self.__deserialize_child_type__()
        self.type = type_info

    def __deserialize_child_type__(self):
        childType = self.__read_type__()
        if childType is not None:
            add_type_to_cache(childType)
        return childType

    def __read_type__(self):
        return super(TypeObject, self).__read_type__()

    def __read_header__(self):
        self.__typeCont = self.__read_int__()
        if self.serializationversion > 0:
            self.__read_int__()

    def __need_read_type__(self):
        return False

    def __need_read_object__(self):
        return False
