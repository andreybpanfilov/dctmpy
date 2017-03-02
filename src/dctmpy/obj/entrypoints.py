#  Copyright (c) 2013 Andrey B. Panfilov <andrew@panfilov.tel>
#
#  See main module for license.
#

from dctmpy.obj.typedobject import TypedObject


class EntryPoints(TypedObject):
    def __init__(self, **kwargs):
        self.__methods = None
        super(EntryPoints, self).__init__(**dict(
            kwargs,
            **{'ser_version': 0}
        ))

    def _read(self, buf=None):
        super(EntryPoints, self)._read(buf)
        if len(self) > 0:
            self.__methods = dict(zip(self['name'], self['pos']))

    def methods(self):
        return self.__methods

    def __getattr__(self, name):
        if self.__methods:
            return self.__methods[name]
        else:
            return super(EntryPoints, self).__getattr__(name)

    def __setattr__(self, name, value):
        super(EntryPoints, self).__setattr__(name, value)


