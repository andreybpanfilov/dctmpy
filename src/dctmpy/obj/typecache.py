class TypeCache:
    class __impl:

        def __init__(self):
            self.__cache = {}

        def get(self, name):
            if name in self.__cache:
                return self.__cache.get(name)
            return None

        def add(self, typeInfo):
            superType = typeInfo.super
            if superType in self.__cache and superType != "NULL":
                typeInfo.extend(self.get(superType))
            self.__cache[typeInfo.name] = typeInfo

    __instance = None

    def __init__(self):
        if not TypeCache.__instance:
            TypeCache.__instance = TypeCache.__impl()
        self.__dict__['_TypeCache__instance'] = TypeCache.__instance

    def get(self, typeName):
        return self.__instance.get(typeName)

    def add(self, typeObj):
        return self.__instance.add(typeObj)
