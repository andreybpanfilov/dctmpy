class TypeInfo(object):
    attributes = ['name', 'id', 'vstamp', 'version', 'cache', 'super',
                  'shared_parent', 'aspect_name', 'aspect_share_flag',
                  'ser_version', 'attrs', 'positions', 'pending']

    def __init__(self, **kwargs):
        for attribute in TypeInfo.attributes:
            setattr(self, attribute, kwargs.pop(attribute, None))
        if self.super == 'NULL':
            self.super = None
        if self.shared_parent == 'NULL':
            self.shared_parent = None
        if not self.super and self.shared_parent:
            self.super = self.shared_parent
        self.pending = self.super
        self.attrs = []
        self.positions = {}

    def append(self, attr_info):
        self.attrs.append(attr_info)
        if self.ser_version <= 0:
            return
        if attr_info.position > -1:
            self.positions[attr_info.position] = attr_info
        elif not self.is_generated():
            raise RuntimeError("Empty position")

    def insert(self, index, attr_info):
        self.attrs.insert(index, attr_info)
        if self.ser_version <= 0:
            return
        if attr_info.position > -1:
            self.positions[attr_info.position] = attr_info
            return
        if not self.is_generated():
            raise RuntimeError("Empty position")

    def get(self, index):
        if self.ser_version > 0:
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
