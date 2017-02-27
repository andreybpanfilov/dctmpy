class TypeInfo(object):
    attributes = ['name', 'id', 'vstamp', 'version', 'cache', 'super',
                  'sharedparent', 'aspectname', 'aspectshareflag',
                  'serversion', 'attrs', 'positions', 'pending']

    def __init__(self, **kwargs):
        for attribute in TypeInfo.attributes:
            setattr(self, attribute, kwargs.pop(attribute, None))
        if self.super == 'NULL':
            self.super = None
        if self.sharedparent == 'NULL':
            self.sharedparent = None
        if not self.super and self.sharedparent:
            self.super = self.sharedparent
        self.pending = self.super
        self.attrs = []
        self.positions = {}

    def append(self, attrInfo):
        self.attrs.append(attrInfo)
        if self.serversion <= 0:
            return
        if attrInfo.position > -1:
            self.positions[attrInfo.position] = attrInfo
        elif not self.is_generated():
            raise RuntimeError("Empty position")

    def insert(self, index, attrInfo):
        self.attrs.insert(index, attrInfo)
        if self.serversion <= 0:
            return
        if attrInfo.position > -1:
            self.positions[attrInfo.position] = attrInfo
            return
        if not self.is_generated():
            raise RuntimeError("Empty position")

    def get(self, index):
        if self.serversion > 0:
            if not self.is_generated():
                return self.positions[index]
        return self.attrs[index]

    def count(self):
        return len(self.attrs)

    def extend(self, other):
        if self.pending != other.name:
            return
        for i in other.attrs[::-1]:
            self.insert(0, i.clone())
        self.pending = other.pending

    def is_generated(self):
        return self.name == "GeneratedType"
