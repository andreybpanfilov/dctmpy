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
            self.__setattr__(ATTRIBUTE_PREFIX + attribute, kwargs.pop(attribute, None))
        super(Collection, self).__init__(**kwargs)

    def __need_read_type__(self):
        return True

    def __need_read_object__(self):
        return False

    def next_record(self):
        if self.collection is None:
            return None

        if isempty(self.buffer) and (self.maybemore is None or self.maybemore):
            response = self.session.next_batch(self.collection, self.batchsize)
            self.buffer = response.data
            self.records = response.records
            self.maybemore = response.maybemore
            if self.serversion > 0:
                self.__read_int__()

        if not isempty(self.buffer) and (self.records is None or self.records > 0):
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
        try:
            if self.collection > 0:
                self.session.close_collection(self.collection)
        finally:
            self.collection = None

    def __del__(self):
        self.close()


class PersistentCollection(Collection):
    def __init__(self, **kwargs):
        super(PersistentCollection, self).__init__(**kwargs)

    def __read__(self, buf=None):
        if isempty(buf) and isempty(self.buffer):
            raise ParserException("Empty data")
        if not isempty(buf):
            self.buffer = buf
        self.type = self.session.get_type(self.__next_string__(), 0)

    def __need_read_type__(self):
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

    def __read_header__(self):
        pass

    def __read__(self, buf=None):
        super(CollectionEntry, self).__read__(buf)
        if self.serversion > 0:
            self.__read_int__()

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

    def __read_header__(self):
        if not self.serversion > 0:
            self.__next_string__()

    def __read__(self, buf=None):
        super(PersistentCollectionEntry, self).__read__(buf)
        if self.serversion > 0:
            self.__read_int__()

    def __getattr__(self, name):
        return super(PersistentCollectionEntry, self).__getattr__(name)

    def __setattr__(self, name, value):
        super(PersistentCollectionEntry, self).__setattr__(name, value)
