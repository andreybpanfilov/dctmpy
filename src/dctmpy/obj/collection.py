# Copyright (c) 2013 Andrey B. Panfilov <andrew@panfilov.tel>
#
# See main module for license.
#

from dctmpy import *
from dctmpy.exceptions import ParserException
from dctmpy.obj.typedobject import TypedObject


class Collection(TypedObject):
    attributes = ['collection', 'batch_size', 'record_count', 'may_be_more', 'persistent']

    def __init__(self, **kwargs):
        for attribute in Collection.attributes:
            setattr(self, attribute, kwargs.pop(attribute, None))
        super(Collection, self).__init__(**kwargs)

    def _need_read_type(self):
        return True

    def _need_read_object(self):
        return False

    def next_record(self):
        if self.collection is None:
            return None

        if self._is_empty() and (self.may_be_more is None or self.may_be_more):
            response = self.session.next_batch(self.collection, self.batch_size)
            self.buffer = response.data
            self.record_count = response.record_count
            self.may_be_more = response.may_be_more
            if self.ser_version > 0 and not is_empty(self.buffer):
                self._read_int()

        if not self._is_empty() and (self.record_count is None or self.record_count > 0):
            try:
                cls = [CollectionEntry, PersistentCollectionEntry][self.persistent]
                entry = cls(session=self.session, type=self.type, buffer=self.buffer)
                self.buffer = entry.buffer
                entry.buffer = None
                return entry
            finally:
                if self.record_count is not None:
                    self.record_count -= 1
        try:
            self.close()
        except Exception, e:
            pass
        return None

    def __iter__(self):
        class iterator(object):
            def __init__(self, obj):
                self.obj = obj
                self.index = -1

            def __iter__(self):
                return self

            def next(self):
                r = self.obj.next_record()
                if r is None:
                    self.obj.close()
                    raise StopIteration
                else:
                    return r

        return iterator(self)

    def close(self):
        if self.collection >= 0:
            try:
                self.session.close_collection(self.collection)
            except:
                pass
            finally:
                self.collection = None

    def __del__(self):
        self.close()


class PersistentCollection(Collection):
    def __init__(self, **kwargs):
        super(PersistentCollection, self).__init__(**kwargs)

    def _read(self, buf=None):
        if is_empty(buf) and is_empty(self.buffer):
            raise ParserException("Empty data")
        if not is_empty(buf):
            self.buffer = buf
        self.type = self.session.get_type(self._next_string(), 0)

    def _need_read_type(self):
        return False


class CollectionEntry(TypedObject):
    def __init__(self, **kwargs):
        super(CollectionEntry, self).__init__(**kwargs)

    def _read_header(self):
        pass

    def _read(self, buf=None):
        super(CollectionEntry, self)._read(buf)
        if self.ser_version > 0:
            self._read_int()


class PersistentCollectionEntry(CollectionEntry):
    def __init__(self, **kwargs):
        super(PersistentCollectionEntry, self).__init__(**kwargs)

    def _read_header(self):
        if not self.ser_version > 0:
            self._next_string()

    def _read(self, buf=None):
        super(PersistentCollectionEntry, self)._read(buf)
        if self.ser_version > 0:
            self._read_int()
