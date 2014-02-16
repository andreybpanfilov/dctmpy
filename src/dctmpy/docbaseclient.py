#  Copyright (c) 2013 Andrey B. Panfilov <andrew@panfilov.tel>
#
#  See main module for license.
#

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
    attributes = ['docbaseid', 'username', 'password', 'messages', 'entrypoints', 'serializationversion', 'iso8601time',
                  'session', 'serializationversionhint', 'docbaseconfig', 'serverconfg', 'faulted', 'knowncommands']

    def __init__(self, **kwargs):
        for attribute in DocbaseClient.attributes:
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
            self.__resolve_docbase_id()
        if self.messages is None:
            self.messages = []
        if self.entrypoints is None:
            self.entrypoints = {
                'ENTRY_POINTS': 0,
                'GET_ERRORS': 558,
            }

        self.__register_known_commands()

        for name in self.entrypoints.keys():
            self.__add_entry_point(name)
        if self.serializationversionhint is None:
            self.serializationversionhint = CLIENT_VERSION_ARRAY[3]

        self.faulted = False

        self.__connect()
        self.fetch_entry_points()

        if self.password is not None and self.username is not None:
            self.authenticate()

    def __resolve_docbase_id(self):
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

    def __reconnect(self):
        ''

    def __connect(self):
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
        server_version = response.next()
        if server_version[7] == DM_CLIENT_SERIALIZATION_VERSION_HINT:
            self.serializationversion = DM_CLIENT_SERIALIZATION_VERSION_HINT
        else:
            self.serializationversion = 0

        if self.serializationversion == 0 or self.serializationversion == 1:
            self.iso8601time = False
        else:
            if server_version[9] & 0x01 != 0:
                self.iso8601time = False
            else:
                self.iso8601time = True

        session = response.next()

        if session == NULL_ID:
            raise RuntimeError(reason)

        self.session = session

    def rpc(self, rpc_id, data=None):
        if not data:
            data = []
        if self.session is not None:
            if len(data) == 0 or data[0] != self.session:
                data.insert(0, self.session)

        (valid, o_data, collection, persistent, more, records) = (None, None, None, None, None, None)

        response = self.request(type=rpc_id, data=data, immediate=True).receive()
        message = response.next()
        o_data = response.last()
        if rpc_id == RPC_APPLY_FOR_OBJECT:
            valid = int(response.next()) > 0
            persistent = int(response.next()) > 0
        elif rpc_id == RPC_APPLY:
            collection = int(response.next())
            persistent = int(response.next()) > 0
            more = int(response.next()) > 0
            valid = collection > 0
        elif rpc_id == RPC_CLOSE_COLLECTION:
            pass
        elif rpc_id == RPC_GET_NEXT_PIECE:
            pass
        elif rpc_id == RPC_MULTI_NEXT:
            records = int(response.next())
            more = int(response.next()) > 0
            valid = int(response.next()) > 0
        else:
            valid = int(response.next()) > 0

        if (o_data & 0x02 != 0) and not self.faulted:
            try:
                self.faulted = True
                self.__get_messages()
            finally:
                self.faulted = False

        if valid is not None and not valid and (o_data & 0x02 != 0) and len(self.messages) > 0:
            reason = ", ".join(
                "%s: %s" % (message['NAME'], message['1']) for message in
                ((lambda x: x.pop(0))(self.messages) for i in xrange(0, len(self.messages)))
                if message['SEVERITY'] == 3
            )
            if len(reason) > 0:
                raise RuntimeError(reason)

        if o_data == 0x10 or (o_data == 0x01 and rpc_id == RPC_GET_NEXT_PIECE):
            message += self.rpc(RPC_GET_NEXT_PIECE).data

        return Response(data=message, odata=o_data, persistent=persistent, collection=collection, more=more,
                        records=records)

    def apply(self, rpc_id, object_id, method, request=None, cls=Collection):
        if rpc_id is None:
            rpc_id = RPC_APPLY

        if object_id is None:
            object_id = NULL_ID

        response = self.rpc(rpc_id, [self.__get_method(method), object_id, request])
        data = response.data

        if rpc_id == RPC_APPLY_FOR_STRING:
            return data
        elif rpc_id == RPC_APPLY_FOR_ID:
            return data
        elif rpc_id == RPC_APPLY_FOR_DOUBLE:
            return data
        elif rpc_id == RPC_APPLY_FOR_BOOL:
            return data
        elif rpc_id == RPC_APPLY_FOR_LONG:
            return data
        elif rpc_id == RPC_APPLY_FOR_TIME:
            return data

        if cls is None:
            if rpc_id == RPC_APPLY:
                cls = Collection
            elif rpc_id == RPC_APPLY_FOR_OBJECT:
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

    def __get_messages(self):
        self.messages = [x for x in self.GET_ERRORS()]

    def authenticate(self, username=None, password=None):
        if username is not None and password is not None:
            self.username = username
            self.password = password
        if self.username is None:
            raise RuntimeError("Empty username")
        if self.password is None:
            raise RuntimeError("Empty password")

        result = self.AUTHENTICATE_USER(self.username, self.__obfuscate(self.password))
        if result['RETURN_VALUE'] != 1:
            raise RuntimeError("Unable to authenticate")

        self.docbaseconfig = self.GET_DOCBASE_CONFIG()
        self.serverconfig = self.GET_SERVER_CONFIG()

    def next_batch(self, collection, batchHint=DEFAULT_BATCH_SIZE):
        return self.rpc(RPC_MULTI_NEXT, [collection, batchHint])

    def close_collection(self, collection):
        self.rpc(RPC_CLOSE_COLLECTION, [collection])

    def fetch_entry_points(self):
        self.entrypoints = self.ENTRY_POINTS().methods()
        for name in self.entrypoints:
            self.__add_entry_point(name)

    def get_server_config(self):
        return self.GET_SERVER_CONFIG()

    def get_docbase_config(self):
        return self.GET_DOCBASE_CONFIG()

    def get_object(self, objectid):
        return self.FETCH(objectid)

    def get_by_qualification(self, qualification):
        collection = self.query("select r_object_id from %s" % qualification)
        record = collection.next_record()
        if record is not None:
            return self.get_object(record['r_object_id'])
        return None

    def get_type(self, name, vstamp=0):
        type_obj = get_type_from_cache(name)
        if type_obj is not None:
            return type_obj
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

    def __obfuscate(self, password):
        if self.__isobfuscated(password):
            return password
        return "".join(
            "%02x" % [x ^ 0xB6, 0xB6][x == 0xB6] for x in (ord(x) for x in password[::-1])
        )

    def __isobfuscated(self, password):
        if re.match("^([0-9a-f]{2})+$", password) is None:
            return False
        for x in re.findall("[0-9a-f]{2}", password):
            if int(x, 16) != 0xB6 and (int(x, 16) ^ 0xB6) > 127:
                return False
        return True

    def __as_object(self, object_id, method, request=None, cls=TypedObject):
        if object_id is None:
            object_id = NULL_ID
        return self.apply(RPC_APPLY_FOR_OBJECT, object_id, method, request, cls)

    def __as_collection(self, object_id, method, request=None, cls=Collection):
        if object_id is None:
            object_id = NULL_ID
        return self.apply(RPC_APPLY, object_id, method, request, cls)

    def __as_string(self, object_id, method, request=None, cls=None):
        if object_id is None:
            object_id = NULL_ID
        return self.apply(RPC_APPLY_FOR_STRING, object_id, method, request, cls)

    def __as_id(self, object_id, method, request=None, cls=None):
        if object_id is None:
            object_id = NULL_ID
        return self.apply(RPC_APPLY_FOR_ID, object_id, method, request, cls)

    def __as_time(self, objectid, method, request=None, cls=None):
        if objectid is None:
            objectid = NULL_ID
        return self.apply(RPC_APPLY_FOR_TIME, objectid, method, request, cls)

    def __get_method(self, name):
        if name not in self.entrypoints:
            raise RuntimeError("Unknown method: %s" % name)
        return self.entrypoints[name]

    def __getattr__(self, name):
        if name in DocbaseClient.attributes:
            return self.__getattribute__(ATTRIBUTE_PREFIX + name)
        else:
            return super(DocbaseClient, self).__getattr__(name)

    def __setattr__(self, name, value):
        if name in DocbaseClient.attributes:
            DocbaseClient.__setattr__(self, ATTRIBUTE_PREFIX + name, value)
        else:
            super(DocbaseClient, self).__setattr__(name, value)

    def __add_entry_point(self, name):
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
                return self.__as_collection(objectid, name, request, cls)

            inner.__name__ = name
            setattr(self.__class__, inner.__name__, inner)

    def __register_known_commands(self):
        cls = self.__class__
        self.__register(
            RpcCommand('GET_SERVER_CONFIG', cls.__as_object, TypedObject, cls.__server_config_request, False))
        self.__register(
            RpcCommand('GET_DOCBASE_CONFIG', cls.__as_object, TypedObject, cls.__docbase_config_request, False))
        self.__register(RpcCommand('ENTRY_POINTS', cls.__as_object, EntryPoints, cls.__entry_points_request, False))
        self.__register(RpcCommand('FETCH', cls.__as_object, Persistent, None, True))
        self.__register(RpcCommand('AUTHENTICATE_USER', cls.__as_object, TypedObject, cls.__auth_request, False))
        self.__register(RpcCommand('GET_ERRORS', cls.__as_collection, Collection, cls.__get_errors_request, False))
        self.__register(RpcCommand('FETCH_TYPE', cls.__as_object, TypedObject, cls.__fetch_type_request, False))
        self.__register(RpcCommand('EXEC', cls.__as_collection, Collection, cls.__query_request, False))
        self.__register(RpcCommand('TIME', cls.__as_time, TypedObject, None, False))
        self.__register(RpcCommand('COUNT_SESSIONS', cls.__as_object, TypedObject, None, False))
        self.__register(RpcCommand('EXEC_SELECT_SQL', cls.__as_collection, Collection, cls.__sql_query_request, False))
        self.__register(
            RpcCommand('FTINDEX_AGENT_ADMIN', cls.__as_object, TypedObject, cls.__index_agent_status_request, False))

    def __register(self, command):
        if not self.knowncommands:
            self.knowncommands = {}
        self.knowncommands[command.command] = command

    @staticmethod
    def __entry_points_request(session):
        obj = TypedObject(session=session)
        obj.add(AttrValue(name="LANGUAGE", type=INT, values=[get_locale_id()]))
        obj.add(AttrValue(name="CHARACTER_SET", type=INT, values=[get_charset_id()]))
        obj.add(AttrValue(name="PLATFORM_ENUM", type=INT, values=[get_platform_id()]))
        obj.add(AttrValue(name="PLATFORM_VERSION_IMAGE", type=STRING, values=["python"]))
        obj.add(AttrValue(name="UTC_OFFSET", type=INT, values=[get_offset_in_seconds()]))
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
    def __auth_request(session, username, password):
        obj = TypedObject(session=session)
        obj.add(AttrValue(name="CONNECT_POOLING", type=BOOL, values=[False]))
        obj.add(AttrValue(name="USER_PASSWORD", type=STRING, values=[password]))
        obj.add(AttrValue(name="AUTHENTICATION_ONLY", type=BOOL, values=[False]))
        obj.add(AttrValue(name="CHECK_ONLY", type=BOOL, values=[False]))
        obj.add(AttrValue(name="LOGON_NAME", type=STRING, values=[username]))
        return obj

    @staticmethod
    def __server_config_request(session):
        obj = TypedObject(session=session)
        obj.add(AttrValue(name="OBJECT_TYPE", type=STRING, values=["dm_server_config"]))
        obj.add(AttrValue(name="FOR_REVERT", type=BOOL, values=[False]))
        obj.add(AttrValue(name="CACHE_VSTAMP", type=INT, values=[0]))
        return obj

    @staticmethod
    def __docbase_config_request(session):
        obj = TypedObject(session=session)
        obj.add(AttrValue(name="OBJECT_TYPE", type=STRING, values=["dm_docbase_config"]))
        obj.add(AttrValue(name="FOR_REVERT", type=BOOL, values=[False]))
        obj.add(AttrValue(name="CACHE_VSTAMP", type=INT, values=[0]))
        return obj

    @staticmethod
    def __fetch_type_request(session, typename, vstamp):
        obj = TypedObject(session=session)
        obj.add(AttrValue(name="TYPE_NAME", type=STRING, values=[typename]))
        obj.add(AttrValue(name="CACHE_VSTAMP", type=INT, values=[vstamp]))
        return obj

    @staticmethod
    def __get_errors_request(session):
        obj = TypedObject(session=session)
        obj.add(AttrValue(name="OBJECT_TYPE", type=STRING, values=["dmError"]))
        return obj

    @staticmethod
    def __query_request(session, query, for_update, batch_hint, bof_dql=False):
        obj = TypedObject(session=session)
        obj.add(AttrValue(name="QUERY", type=STRING, values=[query]))
        obj.add(AttrValue(name="FOR_UPDATE", type=BOOL, values=[for_update]))
        obj.add(AttrValue(name="BATCH_HINT", type=INT, values=[batch_hint]))
        obj.add(AttrValue(name="BOF_DQL", type=BOOL, values=[bof_dql]))
        return obj

    @staticmethod
    def __sql_query_request(session, query, batch_hint):
        obj = TypedObject(session=session)
        obj.add(AttrValue(name="QUERY", type=STRING, values=[query]))
        obj.add(AttrValue(name="BATCH_HINT", type=INT, values=[batch_hint]))
        return obj

    @staticmethod
    def __folder_by_path_request(session, path):
        obj = TypedObject(session=session)
        obj.add(AttrValue(name="_FOLDER_PATH_", type=STRING, values=[path]))
        return obj

    @staticmethod
    def __index_agent_status_request(session, indexname, agentname):
        obj = TypedObject(session=session)
        obj.add(AttrValue(name="NAME", type=STRING, values=[indexname]))
        obj.add(AttrValue(name="AGENT_INSTANCE_NAME", type=STRING, values=[agentname]))
        obj.add(AttrValue(name="ACTION", type=STRING, values=["status"]))
        return obj


class Response(object):
    attributes = ['data', 'odata', 'persistent', 'collection', 'records', 'more']

    def __init__(self, **kwargs):
        for attribute in Response.attributes:
            self.__setattr__(ATTRIBUTE_PREFIX + attribute, kwargs.pop(attribute, None))

    def __getattr__(self, name):
        if name in Response.attributes:
            return self.__getattribute__(ATTRIBUTE_PREFIX + name)
        else:
            return AttributeError("Unknown attribute %s in %s" % (name, str(self.__class__)))

    def __setattr__(self, name, value):
        if name in Response.attributes:
            Response.__setattr__(self, ATTRIBUTE_PREFIX + name, value)
        else:
            super(Response, self).__setattr__(name, value)


class RpcCommand(object):
    attributes = ['command', 'method', 'returntype', 'request', 'needid', 'argcnt']

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
        if name in RpcCommand.attributes:
            return self.__getattribute__(ATTRIBUTE_PREFIX + name)
        else:
            return AttributeError("Unknown attribute %s in %s" % (name, str(self.__class__)))

    def __setattr__(self, name, value):
        if name in RpcCommand.attributes:
            RpcCommand.__setattr__(self, ATTRIBUTE_PREFIX + name, value)
        else:
            super(RpcCommand, self).__setattr__(name, value)


