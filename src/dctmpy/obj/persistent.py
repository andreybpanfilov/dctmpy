#  Copyright (c) 2013 Andrey B. Panfilov <andrew@panfilov.tel>
#
#  See main module for license.
#

from dctmpy import *
from dctmpy.obj.typedobject import TypedObject


class Persistent(TypedObject):
    def __init__(self, **kwargs):
        super(Persistent, self).__init__(**kwargs)

    def __read_type__(self):
        type_name = self.__next_string__(ATTRIBUTE_PATTERN)
        self.__next_string__(ATTRIBUTE_PATTERN)
        stamp = 0
        if self.serializationversion > 0:
            stamp = self.__read_int__()
        return self.session.get_type(type_name, stamp)

    def __read_object__(self):
        return super(Persistent, self).__read_object__()





