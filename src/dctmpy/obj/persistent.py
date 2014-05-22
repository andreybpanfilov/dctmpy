#  Copyright (c) 2013 Andrey B. Panfilov <andrew@panfilov.tel>
#
#  See main module for license.
#

from dctmpy import *
from dctmpy.obj import *
from dctmpy.obj.typedobject import TypedObject


class PersistentProxy(TypedObject):
    def __init__(self, **kwargs):
        super(PersistentProxy, self).__init__(**kwargs)

    def _read_type(self):
        type_name = self._next_string(ATTRIBUTE_PATTERN)
        self._next_string(ATTRIBUTE_PATTERN)
        stamp = 0
        if self.serversion > 0:
            stamp = self._read_int()
        return self.session.get_type(type_name, stamp)

    def _read_object(self):
        return super(PersistentProxy, self)._read_object()


class Persistent(PersistentProxy):
    def __init__(self, **kwargs):
        super(Persistent, self).__init__(**kwargs)

    def object_id(self):
        return self[R_OBJECT_ID]


class SysObject(Persistent):
    def __init__(self, **kwargs):
        super(SysObject, self).__init__(**kwargs)

    def has_content(self):
        return self[R_PAGE_CNT] > 0