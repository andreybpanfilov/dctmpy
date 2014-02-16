#  Copyright (c) 2013 Andrey B. Panfilov <andrew@panfilov.tel>
#
#  See main module for license.
#

import re
from dctmpy import *
from dctmpy.netwise import Netwise
from dctmpy.obj.collection import Collection, PersistentCollection
from dctmpy.obj.entrypoints import EntryPoints
from dctmpy.obj.persistent import Persistent
from dctmpy.obj.type import TypeObject
from dctmpy.obj.typedobject import TypedObject

NETWISE_VERSION = 3
NETWISE_RELEASE = 5
NETWISE_INUMBER = 769


class DocbaseClient(Netwise):
    fields = ['docbaseid', 'username', 'password', 'messages', 'entrypoints', 'serializationversion', 'iso8601time',
              'session', 'serializationversionhint', 'docbaseconfig', 'serverconfg', 'faulted', 'knowncommands']

    def __init__(self, **kwargs):
        for attribute in DocbaseClient.fields:
            self.__setattr__(ATTRIBUTE_PREFIX + attribute, kwargs.pop(attribute, None))
        super(DocbaseClient, self).__init__(**dict(kwargs, **{
            'version': NETWISE_VERSION,
            'release': NETWISE_RELEASE,
            'inumber': NETWISE_INUMBER,
        }))

        if self.serializationversion is None:
            self.serializationversion = 0
        if self.iso8601time is None:
            self.iso8601time = False
        if self.session is None:
            self.session = NULL_ID
        if self.docbaseid is None or self.docbaseid == -1:
            self.resolveDocbaseId()
        if self.messages is None:
            self.messages = []
        if self.entrypoints is None:
            self.entrypoints = {
                'ENTRY_POINTS': 0,
                'GET_ERRORS': 558,
            }

        self.registerKnownCommands()

        for name in self.entrypoints.keys():
            self.addEntryPoint(name)
        if self.serializationversionhint is None:
            self.serializationversionhint = CLIENT_VERSION_ARRAY[3]

        self.faulted = False

        self.connect()
        self.fetchEntryPoints()

        if self.password is not None and self.username is not None:
            self.authenticate()

    def resolveDocbaseId(self):
        response = self.request(
            type=RPC_NEW_SESSION_BY_ADDR,
            data=[
                -1,
                EMPTY_STRING,
                CLIENT_VERSION_STRING,
                EMPTY_STRING,
                CLIENT_VERSION_ARRAY,
                NULL_ID,
            ],
            immediate=True,
        ).receive()

        reason = response.next()
        m = re.search('Wrong docbase id: \(-1\) expecting: \((\d+)\)', reason)
        if m is not None:
            self.docbaseid = int(m.group(1))
        self.disconnect()

    def disconnect(self):
        try:
            if self.session is not None and self.session != NULL_ID:
                self.request(
                    type=RPC_CLOSE_SESSION,
                    data=[
                        self.session,
                    ],
                    immediate=True,
                ).receive()
            super(DocbaseClient, self).disconnect()
        finally:
            self.session = None

    def reconnect(self):
        ''

    def connect(self):
        response = self.request(
            type=RPC_NEW_SESSION_BY_ADDR,
            data=[
                self.docbaseid,
                EMPTY_STRING,
                CLIENT_VERSION_STRING,
                EMPTY_STRING,
                CLIENT_VERSION_ARRAY,
                NULL_ID,
            ],
            immediate=True,
        ).receive()

        reason = response.next()
        serverVersion = response.next()
        if serverVersion[7] == DM_CLIENT_SERIALIZATION_VERSION_HINT:
            self.serializationversion = DM_CLIENT_SERIALIZATION_VERSION_HINT
        else:
            self.serializationversion = 0

        if self.serializationversion == 0 or self.serializationversion == 1:
            self.iso8601time = False
        else:
            if serverVersion[9] & 0x01 != 0:
                self.iso8601time = False
            else:
                self.iso8601time = True

        session = response.next()

        if session == NULL_ID:
            raise RuntimeError(reason)

        self.session = session

    def rpc(self, rpcid, data=None):
        if not data:
            data = []
        if self.session is not None:
            if len(data) == 0 or data[0] != self.session:
                data.insert(0, self.session)

        (valid, odata, collection, persistent, more, records) = (None, None, None, None, None, None)

        response = self.request(type=rpcid, data=data, immediate=True).receive()
        message = response.next()
        odata = response.last()
        if rpcid == RPC_APPLY_FOR_OBJECT:
            valid = int(response.next()) > 0
            persistent = int(response.next()) > 0
        elif rpcid == RPC_APPLY:
            collection = int(response.next())
            persistent = int(response.next()) > 0
            more = int(response.next()) > 0
            valid = collection > 0
        elif rpcid == RPC_CLOSE_COLLECTION:
            pass
        elif rpcid == RPC_GET_NEXT_PIECE:
            pass
        elif rpcid == RPC_MULTI_NEXT:
            records = int(response.next())
            more = int(response.next()) > 0
            valid = int(response.next()) > 0
        else:
            valid = int(response.next()) > 0

        if (odata & 0x02 != 0) and not self.faulted:
            try:
                self.faulted = True
                self.getMessages()
            finally:
                self.faulted = False

        if valid is not None and not valid and (odata & 0x02 != 0) and len(self.messages) > 0:
            reason = ", ".join(
                "%s: %s" % (message['NAME'], message['1']) for message in
                ((lambda x: x.pop(0))(self.messages) for i in xrange(0, len(self.messages)))
                if message['SEVERITY'] == 3
            )
            if len(reason) > 0:
                raise RuntimeError(reason)

        if odata == 0x10 or (odata == 0x01 and rpcid == RPC_GET_NEXT_PIECE):
            message += self.rpc(RPC_GET_NEXT_PIECE).data

        return Response(data=message, odata=odata, persistent=persistent, collection=collection, more=more,
                        records=records)

    def apply(self, rpcid, objectid, method, request=None, cls=Collection):
        if rpcid is None:
            rpcid = RPC_APPLY

        if objectid is None:
            objectid = NULL_ID

        response = self.rpc(rpcid, [self.getMethod(method), objectid, request])
        data = response.data

        if rpcid == RPC_APPLY_FOR_STRING:
            return data
        elif rpcid == RPC_APPLY_FOR_ID:
            return data
        elif rpcid == RPC_APPLY_FOR_DOUBLE:
            return data
        elif rpcid == RPC_APPLY_FOR_BOOL:
            return data
        elif rpcid == RPC_APPLY_FOR_LONG:
            return data
        elif rpcid == RPC_APPLY_FOR_TIME:
            return data

        if cls is None:
            if rpcid == RPC_APPLY:
                cls = Collection
            elif rpcid == RPC_APPLY_FOR_OBJECT:
                cls = TypedObject

        if response.persistent:
            if cls == Collection:
                cls = PersistentCollection
            elif cls == TypedObject:
                cls = Persistent

        result = cls(session=self, buffer=data)
        if response.collecton is not None and isinstance(result, Collection):
            result.collection = response.collection
            result.persistent = response.persistent
            result.records = response.records
            result.more = response.more
            if isinstance(request, TypedObject) and 'BATCH_HINT' in request:
                result.batchsize = request['BATCH_HINT']
            else:
                result.batchsize = DEFAULT_BATCH_SIZE

        return result

    def getMessages(self):
        self.messages = [x for x in self.GET_ERRORS()]

    def authenticate(self, username=None, password=None):
        if username is not None and password is not None:
            self.username = username
            self.password = password
        if self.username is None:
            raise RuntimeError("Empty username")
        if self.password is None:
            raise RuntimeError("Empty password")

        result = self.AUTHENTICATE_USER(self.username, self.obfuscate(self.password))
        if result['RETURN_VALUE'] != 1:
            raise RuntimeError("Unable to authenticate")

        self.docbaseconfig = self.GET_DOCBASE_CONFIG()
        self.serverconfig = self.GET_SERVER_CONFIG()

    def nextBatch(self, collection, batchHint=DEFAULT_BATCH_SIZE):
        return self.rpc(RPC_MULTI_NEXT, [collection, batchHint])

    def closeCollection(self, collection):
        self.rpc(RPC_CLOSE_COLLECTION, [collection])

    def fetchEntryPoints(self):
        self.entrypoints = self.ENTRY_POINTS().methods()
        for name in self.entrypoints:
            self.addEntryPoint(name)

    def getServerConfig(self):
        return self.GET_SERVER_CONFIG()

    def getDocbaseConfig(self):
        return self.GET_DOCBASE_CONFIG()

    def fetch(self, objectid):
        return self.FETCH(objectid)

    def qualification(self, qualification):
        collection = self.query("select r_object_id from %s" % qualification)
        record = collection.nextRecord()
        if record is not None:
            return self.fetch(record['r_object_id'])
        return None

    def fetchType(self, name, vstamp=0):
        typeObj = getTypeFormCache(name)
        if typeObj is not None:
            return typeObj
        data = None
        if "FETCH_TYPE" in self.entrypoints:
            data = self.FETCH_TYPE(name, vstamp)['result']
        else:
            data = self.rpc(RPC_FETCH_TYPE, [name]).data
        return TypeObject(session=self, buffer=data).type

    def query(self, query, forUpdate=False, batchHint=DEFAULT_BATCH_SIZE, bofDQL=False):
        try:
            collection = self.EXEC(query, forUpdate, batchHint, bofDQL)
        except Exception, e:
            raise RuntimeError("Error occurred while executing query: %s" % query, e)
        return collection

    def obfuscate(self, password):
        if self.isObfuscated(password):
            return password
        return "".join(
            "%02x" % [x ^ 0xB6, 0xB6][x == 0xB6] for x in (ord(x) for x in password[::-1])
        )

    def isObfuscated(self, password):
        if re.match("^([0-9a-f]{2})+$", password) is None:
            return False
        for x in re.findall("[0-9a-f]{2}", password):
            if int(x, 16) != 0xB6 and (int(x, 16) ^ 0xB6) > 127:
                return False
        return True

    def asObject(self, objectid, method, request=None, cls=TypedObject):
        if objectid is None:
            objectid = NULL_ID
        return self.apply(RPC_APPLY_FOR_OBJECT, objectid, method, request, cls)

    def asCollection(self, objectid, method, request=None, cls=Collection):
        if objectid is None:
            objectid = NULL_ID
        return self.apply(RPC_APPLY, objectid, method, request, cls)

    def asString(self, objectid, method, request=None, cls=None):
        if objectid is None:
            objectid = NULL_ID
        return self.apply(RPC_APPLY_FOR_STRING, objectid, method, request, cls)

    def asId(self, objectid, method, request=None, cls=None):
        if objectid is None:
            objectid = NULL_ID
        return self.apply(RPC_APPLY_FOR_ID, objectid, method, request, cls)

    def asTime(self, objectid, method, request=None, cls=None):
        if objectid is None:
            objectid = NULL_ID
        return self.apply(RPC_APPLY_FOR_TIME, objectid, method, request, cls)

    def getMethod(self, name):
        if name not in self.entrypoints:
            raise RuntimeError("Unknown method: %s" % name)
        return self.entrypoints[name]

    def __getattr__(self, name):
        if name in DocbaseClient.fields:
            return self.__getattribute__(ATTRIBUTE_PREFIX + name)
        else:
            return super(DocbaseClient, self).__getattr__(name)

    def __setattr__(self, name, value):
        if name in DocbaseClient.fields:
            DocbaseClient.__setattr__(self, ATTRIBUTE_PREFIX + name, value)
        else:
            super(DocbaseClient, self).__setattr__(name, value)

    def addEntryPoint(self, name):
        if getattr(DocbaseClient, name, None) is not None:
            return
        elif name in self.knowncommands:
            command = self.knowncommands[name]
            method = command.method
            cls = command.returntype
            request = command.request
            needid = command.needid
            argc = command.argcnt
            if needid:
                def inner(self, objectid=NULL_ID, *args):
                    if not request:
                        return method(self, objectid, name, None, cls)
                    if argc == 1:
                        return method(self, objectid, name, request(self), cls)
                    else:
                        return method(self, objectid, name, request(self, *args), cls)
            else:
                def inner(self, *args):
                    if not request:
                        return method(self, NULL_ID, name, None, cls)
                    if argc == 1:
                        return method(self, NULL_ID, name, request(self), cls)
                    else:
                        return method(self, NULL_ID, name, request(self, *args), cls)

            inner.__name__ = name
            setattr(self.__class__, inner.__name__, inner)
        else:
            def inner(self, objectid=NULL_ID, request=None, cls=Collection):
                return self.asCollection(objectid, name, request, cls)

            inner.__name__ = name
            setattr(self.__class__, inner.__name__, inner)

    def registerKnownCommands(self):
        cls = self.__class__
        self.register(RpcCommand('GET_SERVER_CONFIG', cls.asObject, TypedObject, cls.serverConfigRequest, False))
        self.register(RpcCommand('GET_DOCBASE_CONFIG', cls.asObject, TypedObject, cls.docbaseConfigRequest, False))
        self.register(RpcCommand('ENTRY_POINTS', cls.asObject, EntryPoints, cls.entryPointsRequest, False))
        self.register(RpcCommand('FETCH', cls.asObject, Persistent, None, True))
        self.register(RpcCommand('AUTHENTICATE_USER', cls.asObject, TypedObject, cls.authRequest, False))
        self.register(RpcCommand('GET_ERRORS', cls.asCollection, Collection, cls.getErrorsRequest, False))
        self.register(RpcCommand('FETCH_TYPE', cls.asObject, TypedObject, cls.fetchTypeRequest, False))
        self.register(RpcCommand('EXEC', cls.asCollection, Collection, cls.queryRequest, False))
        self.register(RpcCommand('TIME', cls.asTime, TypedObject, None, False))
        self.register(RpcCommand('COUNT_SESSIONS', cls.asObject, TypedObject, None, False))
        self.register(RpcCommand('EXEC_SELECT_SQL', cls.asCollection, Collection, cls.sqlQueryRequest, False))
        self.register(RpcCommand('FTINDEX_AGENT_ADMIN', cls.asObject, TypedObject, cls.indexAgentStatusRequest, False))

    def register(self, command):
        if not self.knowncommands:
            self.knowncommands = {}
        self.knowncommands[command.command] = command

    @staticmethod
    def entryPointsRequest(session):
        obj = TypedObject(session=session)
        obj.add(AttrValue(name="LANGUAGE", type=INT, values=[getLocaleId()]))
        obj.add(AttrValue(name="CHARACTER_SET", type=INT, values=[getCharsetId()]))
        obj.add(AttrValue(name="PLATFORM_ENUM", type=INT, values=[getPlatformId()]))
        obj.add(AttrValue(name="PLATFORM_VERSION_IMAGE", type=STRING, values=["python"]))
        obj.add(AttrValue(name="UTC_OFFSET", type=INT, values=[getOffsetInSeconds()]))
        obj.add(AttrValue(name="SDF_AN_custom_date_order", type=INT, values=[0]))
        obj.add(AttrValue(name="SDF_AN_custom_scan_fields", type=INT, values=[0]))
        obj.add(AttrValue(name="SDF_AN_date_separator", type=STRING, values=["/"]))
        obj.add(AttrValue(name="SDF_AN_date_order", type=INT, values=[2]))
        obj.add(AttrValue(name="SDF_AN_day_leading_zero", type=BOOL, values=[True]))
        obj.add(AttrValue(name="SDF_AN_month_leading_zero", type=BOOL, values=[True]))
        obj.add(AttrValue(name="SDF_AN_century", type=BOOL, values=[True]))
        obj.add(AttrValue(name="SDF_AN_time_separator", type=STRING, values=[":"]))
        obj.add(AttrValue(name="SDF_AN_hours_24", type=BOOL, values=[True]))
        obj.add(AttrValue(name="SDF_AN_hour_leading_zero", type=BOOL, values=[True]))
        obj.add(AttrValue(name="SDF_AN_noon_is_zero", type=BOOL, values=[False]))
        obj.add(AttrValue(name="SDF_AN_am", type=STRING, values=["AM"]))
        obj.add(AttrValue(name="SDF_AN_pm", type=STRING, values=["PM"]))
        obj.add(AttrValue(name="PLATFORM_EXTRA", type=INT, repeating=True, values=[0, 0, 0, 0]))
        obj.add(AttrValue(name="APPLICATION_CODE", type=STRING, values=[""]))
        return obj

    @staticmethod
    def authRequest(session, username, password):
        obj = TypedObject(session=session)
        obj.add(AttrValue(name="CONNECT_POOLING", type=BOOL, values=[False]))
        obj.add(AttrValue(name="USER_PASSWORD", type=STRING, values=[password]))
        obj.add(AttrValue(name="AUTHENTICATION_ONLY", type=BOOL, values=[False]))
        obj.add(AttrValue(name="CHECK_ONLY", type=BOOL, values=[False]))
        obj.add(AttrValue(name="LOGON_NAME", type=STRING, values=[username]))
        return obj

    @staticmethod
    def serverConfigRequest(session):
        obj = TypedObject(session=session)
        obj.add(AttrValue(name="OBJECT_TYPE", type=STRING, values=["dm_server_config"]))
        obj.add(AttrValue(name="FOR_REVERT", type=BOOL, values=[False]))
        obj.add(AttrValue(name="CACHE_VSTAMP", type=INT, values=[0]))
        return obj

    @staticmethod
    def docbaseConfigRequest(session):
        obj = TypedObject(session=session)
        obj.add(AttrValue(name="OBJECT_TYPE", type=STRING, values=["dm_docbase_config"]))
        obj.add(AttrValue(name="FOR_REVERT", type=BOOL, values=[False]))
        obj.add(AttrValue(name="CACHE_VSTAMP", type=INT, values=[0]))
        return obj

    @staticmethod
    def fetchTypeRequest(session, typename, vstamp):
        obj = TypedObject(session=session)
        obj.add(AttrValue(name="TYPE_NAME", type=STRING, values=[typename]))
        obj.add(AttrValue(name="CACHE_VSTAMP", type=INT, values=[vstamp]))
        return obj

    @staticmethod
    def getErrorsRequest(session):
        obj = TypedObject(session=session)
        obj.add(AttrValue(name="OBJECT_TYPE", type=STRING, values=["dmError"]))
        return obj

    @staticmethod
    def queryRequest(session, query, forUpdate, batchHint, bofDql):
        obj = TypedObject(session=session)
        obj.add(AttrValue(name="QUERY", type=STRING, values=[query]))
        obj.add(AttrValue(name="FOR_UPDATE", type=BOOL, values=[forUpdate]))
        obj.add(AttrValue(name="BATCH_HINT", type=INT, values=[batchHint]))
        obj.add(AttrValue(name="BOF_DQL", type=BOOL, values=[bofDql]))
        return obj

    @staticmethod
    def sqlQueryRequest(session, query, batchHint):
        obj = TypedObject(session=session)
        obj.add(AttrValue(name="QUERY", type=STRING, values=[query]))
        obj.add(AttrValue(name="BATCH_HINT", type=INT, values=[batchHint]))
        return obj

    @staticmethod
    def folderByPathRequest(session, path):
        obj = TypedObject(session=session)
        obj.add(AttrValue(name="_FOLDER_PATH_", type=STRING, values=[path]))
        return obj

    @staticmethod
    def indexAgentStatusRequest(session, indexname, agentname):
        obj = TypedObject(session=session)
        obj.add(AttrValue(name="NAME", type=STRING, values=[indexname]))
        obj.add(AttrValue(name="AGENT_INSTANCE_NAME", type=STRING, values=[agentname]))
        obj.add(AttrValue(name="ACTION", type=STRING, values=["status"]))
        return obj


class Response(object):
    fields = ['data', 'odata', 'persistent', 'collection', 'records', 'more']

    def __init__(self, **kwargs):
        for attribute in Response.fields:
            self.__setattr__(ATTRIBUTE_PREFIX + attribute, kwargs.pop(attribute, None))

    def __getattr__(self, name):
        if name in Response.fields:
            return self.__getattribute__(ATTRIBUTE_PREFIX + name)
        else:
            return AttributeError

    def __setattr__(self, name, value):
        if name in Response.fields:
            Response.__setattr__(self, ATTRIBUTE_PREFIX + name, value)
        else:
            super(Response, self).__setattr__(name, value)


class RpcCommand(object):
    fields = ['command', 'method', 'returntype', 'request', 'needid', 'argcnt']

    def __init__(self, command, method, returntype, request, needid):
        self.command = command
        self.method = method
        self.returntype = returntype
        self.request = request
        self.needid = needid
        if self.request:
            self.argcnt = self.request.func_code.co_argcount
        else:
            self.argcnt = 0

    def __getattr__(self, name):
        if name in RpcCommand.fields:
            return self.__getattribute__(ATTRIBUTE_PREFIX + name)
        else:
            return AttributeError

    def __setattr__(self, name, value):
        if name in RpcCommand.fields:
            RpcCommand.__setattr__(self, ATTRIBUTE_PREFIX + name, value)
        else:
            super(RpcCommand, self).__setattr__(name, value)


