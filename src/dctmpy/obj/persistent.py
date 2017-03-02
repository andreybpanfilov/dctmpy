# Copyright (c) 2013 Andrey B. Panfilov <andrew@panfilov.tel>
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
        if self.ser_version > 0:
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
        content = self.session.get_object(self.session.convert_id(objectId, fmt, page, page_modifier))
        for chunk in content.get_content(objectId):
            yield chunk


class DmDocument(DmSysObject):
    def __init__(self, **kwargs):
        super(DmDocument, self).__init__(**kwargs)


class DmPlugin(DmSysObject):
    def __init__(self, **kwargs):
        super(DmPlugin, self).__init__(**kwargs)


class DmQuery(DmSysObject):
    def __init__(self, **kwargs):
        super(DmQuery, self).__init__(**kwargs)


class DmMethod(DmSysObject):
    def __init__(self, **kwargs):
        super(DmMethod, self).__init__(**kwargs)


class DmiExprCode(DmSysObject):
    def __init__(self, **kwargs):
        super(DmiExprCode, self).__init__(**kwargs)


class DmOutputDevice(DmSysObject):
    def __init__(self, **kwargs):
        super(DmOutputDevice, self).__init__(**kwargs)


class DmRouter(DmSysObject):
    def __init__(self, **kwargs):
        super(DmRouter, self).__init__(**kwargs)


class DmRegistered(DmSysObject):
    def __init__(self, **kwargs):
        super(DmRegistered, self).__init__(**kwargs)


class DmServerConfig(DmSysObject):
    def __init__(self, **kwargs):
        super(DmServerConfig, self).__init__(**kwargs)


class DmDocbaseConfig(DmSysObject):
    def __init__(self, **kwargs):
        super(DmDocbaseConfig, self).__init__(**kwargs)


class DmPolicy(DmSysObject):
    def __init__(self, **kwargs):
        super(DmPolicy, self).__init__(**kwargs)


class DmProcess(DmSysObject):
    def __init__(self, **kwargs):
        super(DmProcess, self).__init__(**kwargs)


class DmActivity(DmSysObject):
    def __init__(self, **kwargs):
        super(DmActivity, self).__init__(**kwargs)


class DmNote(DmSysObject):
    def __init__(self, **kwargs):
        super(DmNote, self).__init__(**kwargs)


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
                try:
                    self.session.kill_puller(handle)
                except:
                    pass


TAG_CLASS_MAPPING = {
    6: DmrContent,
    8: DmSysObject,
    9: DmDocument,
    10: DmQuery,
    11: DmFolder,
    12: DmCabinet,
    16: DmMethod,
    23: DmOutputDevice,
    24: DmRouter,
    25: DmRegistered,
    60: DmDocbaseConfig,
    61: DmServerConfig,
    65: DmNote,
    70: DmPolicy,
    75: DmProcess,
    76: DmActivity,
    88: DmiExprCode,
    103: DmPlugin,
}