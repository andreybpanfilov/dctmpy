#  Copyright (c) 2013 Andrey B. Panfilov <andrew@panfilov.tel>
#
#  See main module for license.
#

from dctmpy import *
from dctmpy.obj.typedobject import TypedObject


class Persistent(TypedObject):
    def __init__(self, **kwargs):
        super(Persistent, self).__init__(**kwargs)

    def _read_type(self):
        type_name = self._next_string(ATTRIBUTE_PATTERN)
        self._next_string(ATTRIBUTE_PATTERN)
        stamp = 0
        if self.serversion > 0:
            stamp = self._read_int()
        return self.session.get_type(type_name, stamp)

    def _read_object(self):
        return super(Persistent, self)._read_object()





