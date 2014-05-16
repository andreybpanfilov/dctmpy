#  Copyright (c) 2013 Andrey B. Panfilov <andrew@panfilov.tel>
#
#  See main module for license.
#

from dctmpy import *
from dctmpy.obj.typedobject import TypedObject


class Collection(TypedObject):
    attributes = ['collection', 'batchsize', 'records', 'maybemore', 'persistent']

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

        if is_empty(self.buffer) and (self.maybemore is None or self.maybemore):
            response = self.session.next_batch(self.collection, self.batchsize)
            self.buffer = response.data
            self.records = response.records
            self.maybemore = response.maybemore
            if self.serversion > 0 and not is_empty(self.buffer):
                self._read_int()

        if not is_empty(self.buffer) and (self.records is None or self.records > 0):
            try:
                cls = [CollectionEntry, PersistentCollectionEntry][self.persistent]
                entry = cls(session=self.session, type=self.type, buffer=self.buffer)
                self.buffer = entry.buffer
                return entry
            finally:
                if self.records is not None:
                    self.records -= 1
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
                    raise StopIteration
                else:
                    return r

        return iterator(self)

    def __getattr__(self, name):
        if name in Collection.attributes:
            return self.__getattribute__(ATTRIBUTE_PREFIX + name)
        else:
            return super(Collection, self).__getattr__(name)

    def __setattr__(self, name, value):
        if name in Collection.attributes:
            Collection.__setattr__(self, ATTRIBUTE_PREFIX + name, value)
        else:
            super(Collection, self).__setattr__(name, value)

    def close(self):
        if self.collection >= 0:
            try:
                self.session.close_collection(self.collection)
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

    def next_record(self):
        return super(PersistentCollection, self).next_record()

    def __getattr__(self, name):
        return super(PersistentCollection, self).__getattr__(name)

    def __setattr__(self, name, value):
        super(PersistentCollection, self).__setattr__(name, value)


class CollectionEntry(TypedObject):
    def __init__(self, **kwargs):
        super(CollectionEntry, self).__init__(**kwargs)

    def _read_header(self):
        pass

    def _read(self, buf=None):
        super(CollectionEntry, self)._read(buf)
        if self.serversion > 0:
            self._read_int()

    def __getattr__(self, name):
        if name in CollectionEntry.attributes:
            return self.__getattribute__(ATTRIBUTE_PREFIX + name)
        else:
            return super(CollectionEntry, self).__getattr__(name)

    def __setattr__(self, name, value):
        if name in CollectionEntry.attributes:
            CollectionEntry.__setattr__(self, ATTRIBUTE_PREFIX + name, value)
        else:
            super(CollectionEntry, self).__setattr__(name, value)


class PersistentCollectionEntry(CollectionEntry):
    def __init__(self, **kwargs):
        super(PersistentCollectionEntry, self).__init__(**kwargs)

    def _read_header(self):
        if not self.serversion > 0:
            self._next_string()

    def _read(self, buf=None):
        super(PersistentCollectionEntry, self)._read(buf)
        if self.serversion > 0:
            self._read_int()

    def __getattr__(self, name):
        return super(PersistentCollectionEntry, self).__getattr__(name)

    def __setattr__(self, name, value):
        super(PersistentCollectionEntry, self).__setattr__(name, value)
