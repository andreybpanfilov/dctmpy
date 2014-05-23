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
        cls = self._get_class()
        if cls is not None:
            self.__class__ = cls

    def _read_type(self):
        type_name = self._next_string(ATTRIBUTE_PATTERN)
        self._next_string(ATTRIBUTE_PATTERN)
        stamp = 0
        if self.serversion > 0:
            stamp = self._read_int()
        return self.session.get_type(type_name, stamp)

    def _read_object(self):
        return super(PersistentProxy, self)._read_object()

    def _get_class(self):
        return TAG_CLASS_MAPPING.get(self._get_type_id(), Persistent)

    def _get_type_id(self):
        if is_empty(self[R_OBJECT_ID]):
            return 0
        return int(self[R_OBJECT_ID][:2], 16)


class Persistent(PersistentProxy):
    def __init__(self, **kwargs):
        super(Persistent, self).__init__(**kwargs)

    def object_id(self):
        return self[R_OBJECT_ID]


class DmSysObject(Persistent):
    def __init__(self, **kwargs):
        super(DmSysObject, self).__init__(**kwargs)

    def has_content(self):
        return self[R_PAGE_CNT] > 0

    def get_content(self, page=0, fmt=None, page_modifier=''):
        if fmt is None:
            fmt = self[A_CONTENT_TYPE]
        objectId = self.object_id()
        content = self.session.fetch(self.session.convert_id(objectId, fmt, page, page_modifier))
        for chunk in content.get_content(objectId):
            yield chunk


class DmDocument(DmSysObject):
    def __init__(self, **kwargs):
        super(DmDocument, self).__init__(**kwargs)


class DmFolder(DmSysObject):
    def __init__(self, **kwargs):
        super(DmFolder, self).__init__(**kwargs)


class DmCabinet(DmFolder):
    def __init__(self, **kwargs):
        super(DmCabinet, self).__init__(**kwargs)


class DmrContent(Persistent):
    def __init__(self, **kwargs):
        super(DmrContent, self).__init__(**kwargs)

    def get_content(self, objectId=NULL_ID):
        handle = 0
        try:
            handle = self.session.make_puller(
                objectId, self[STORAGE_ID], self.object_id(), self[FORMAT], self[DATA_TICKET]
            )
            if handle == 0:
                raise RuntimeError("Unable make puller")
            for chunk in self.session.download(handle):
                yield chunk
        finally:
            if handle > 0:
                self.session.kill_puller(handle)


TAG_CLASS_MAPPING = {
    6: DmrContent,
    8: DmSysObject,
    9: DmDocument,
    11: DmFolder,
    12: DmCabinet,
}