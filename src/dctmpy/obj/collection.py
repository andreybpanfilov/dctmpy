#  Copyright (c) 2013 Andrey B. Panfilov <andrew@panfilov.tel>
#
#  See main module for license.
#

from dctmpy import *
from dctmpy.obj.typedobject import TypedObject


class Collection(TypedObject):
    fields = ['collection', 'batchsize', 'records', 'more', 'persistent']

    def __init__(self, **kwargs):
        for attribute in Collection.fields:
            self.__setattr__(ATTRIBUTE_PREFIX + attribute, kwargs.pop(attribute, None))
        super(Collection, self).__init__(**kwargs)

    def needReadType(self):
        return True

    def needReadObject(self):
        return False

    def nextRecord(self):
        if self.collection is None:
            return None

        if isEmpty(self.buffer) and (self.more is None or self.more):
            response = self.session.nextBatch(self.collection, self.batchsize)
            self.buffer = response.data
            self.records = response.records
            self.more = response.more
            if self.serializationversion > 0:
                self.readInt()

        if not isEmpty(self.buffer) and (self.records is None or self.records > 0):
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
                r = self.obj.nextRecord()
                if r is None:
                    raise StopIteration
                else:
                    return r

        return iterator(self)

    def __getattr__(self, name):
        if name in Collection.fields:
            return self.__getattribute__(ATTRIBUTE_PREFIX + name)
        else:
            return super(Collection, self).__getattr__(name)

    def __setattr__(self, name, value):
        if name in Collection.fields:
            Collection.__setattr__(self, ATTRIBUTE_PREFIX + name, value)
        else:
            super(Collection, self).__setattr__(name, value)

    def close(self):
        try:
            if self.collection > 0:
                self.session.closeCollection(self.collection)
        finally:
            self.collection = None

    def __del__(self):
        self.close()


class PersistentCollection(Collection):
    def __init__(self, **kwargs):
        super(PersistentCollection, self).__init__(**kwargs)

    def read(self, buf=None):
        if isEmpty(buf) and isEmpty(self.buffer):
            raise ParserException("Empty data")
        if not isEmpty(buf):
            self.buffer = buf
        self.type = self.session.fetchType(self.nextString(), 0)

    def needReadType(self):
        return False

    def nextRecord(self):
        return super(PersistentCollection, self).nextRecord()

    def __getattr__(self, name):
        return super(PersistentCollection, self).__getattr__(name)

    def __setattr__(self, name, value):
        super(PersistentCollection, self).__setattr__(name, value)


class CollectionEntry(TypedObject):
    def __init__(self, **kwargs):
        super(CollectionEntry, self).__init__(**kwargs)

    def readHeader(self):
        pass

    def read(self, buf=None):
        super(CollectionEntry, self).read(buf)
        if self.serializationversion > 0:
            self.readInt()

    def __getattr__(self, name):
        if name in CollectionEntry.fields:
            return self.__getattribute__(ATTRIBUTE_PREFIX + name)
        else:
            return super(CollectionEntry, self).__getattr__(name)

    def __setattr__(self, name, value):
        if name in CollectionEntry.fields:
            CollectionEntry.__setattr__(self, ATTRIBUTE_PREFIX + name, value)
        else:
            super(CollectionEntry, self).__setattr__(name, value)


class PersistentCollectionEntry(CollectionEntry):
    def __init__(self, **kwargs):
        super(PersistentCollectionEntry, self).__init__(**kwargs)

    def readHeader(self):
        if not self.serializationversion > 0:
            self.nextString()

    def read(self, buf=None):
        super(PersistentCollectionEntry, self).read(buf)
        if self.serializationversion > 0:
            self.readInt()

    def __getattr__(self, name):
        return super(PersistentCollectionEntry, self).__getattr__(name)

    def __setattr__(self, name, value):
        super(PersistentCollectionEntry, self).__setattr__(name, value)
