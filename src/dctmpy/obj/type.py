#  Copyright (c) 2013 Andrey B. Panfilov <andrew@panfilov.tel>
#
#  See main module for license.
#

from dctmpy import *
from dctmpy.obj.typedobject import TypedObject


class TypeObject(TypedObject):
    def __init__(self, **kwargs):
        self._type_count = None
        super(TypeObject, self).__init__(**kwargs)

    def _read(self, buf=None):
        self._read_header()
        type_info = None
        if self._type_count is not None:
            for i in xrange(0, self._type_count):
                type_info = self._deserialize_child_type()
        else:
            while not is_empty(self.buffer()):
                type_info = self._deserialize_child_type()
        self.type = type_info

    def _deserialize_child_type(self):
        child_type = self._read_type()
        if child_type is not None:
            add_type_to_cache(child_type)
        return child_type

    def _read_type(self):
        return super(TypeObject, self)._read_type()

    def _read_header(self):
        self._type_count = self._read_int()
        if self.ser_version > 0:
            self._read_int()

    def _need_read_type(self):
        return False

    def _need_read_object(self):
        return False
