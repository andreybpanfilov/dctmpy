class AttrInfo(object):
    attributes = ['position', 'name', 'type', 'repeating', 'length', 'restriction', 'extended']

    def __init__(self, **kwargs):
        for attribute in AttrInfo.attributes:
            setattr(self, attribute, kwargs.pop(attribute, None))

    def clone(self):
        return AttrInfo(**dict((x, getattr(self, x)) for x in AttrInfo.attributes))
