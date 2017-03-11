from dctmpy import as_list


class AttrValue(object):
    attributes = ['name', 'type', 'length', 'repeating', 'values', 'extended']

    def __init__(self, **kwargs):
        for attribute in AttrValue.attributes:
            setattr(self, attribute, kwargs.pop(attribute, None))
        self.values = as_list(self.values)
        if self.repeating is None:
            self.repeating = False
        if self.length is None:
            self.length = 0

    def __len__(self):
        if self.repeating:
            return len(self.values)
        return 1

    def __getitem__(self, key):
        if isinstance(key, slice):
            return [self[x] for x in xrange(*key.indices(len(self)))]
        if not isinstance(key, int):
            raise TypeError("Invalid argument type")
        if self.repeating:
            if key > len(self.values):
                raise KeyError
            return self.values[key]
        if key > 0:
            raise KeyError
        if len(self.values) == 0:
            return None
        return self.values[0]

    def __iter__(self):
        class Iterator(object):
            def __init__(self, obj):
                self.obj = obj
                self.index = -1

            def __iter__(self):
                return self

            def next(self):
                if self.index >= len(self.obj):
                    raise StopIteration
                self.index += 1
                return self.obj[self.index]

        return Iterator(self)
