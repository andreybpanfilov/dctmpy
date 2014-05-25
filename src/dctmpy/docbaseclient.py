#  Copyright (c) 2013 Andrey B. Panfilov <andrew@panfilov.tel>
#
#  See main module for license.
#
import logging

from dctmpy import *
from dctmpy.net.request import Request, DownloadRequest
from dctmpy.netwise import Netwise
from dctmpy.obj.collection import Collection, PersistentCollection
from dctmpy.obj.persistent import PersistentProxy
from dctmpy.obj.type import TypeObject
from dctmpy.obj.typedobject import TypedObject
from dctmpy.rpc.rpccommands import Rpc

NETWISE_VERSION = 3
NETWISE_RELEASE = 5
NETWISE_INUMBER = 769

DEFAULT_CHARSET = 'UTF-8'


class DocbaseClient(Netwise):
    attributes = ['docbaseid', 'username', 'password', 'messages', 'entrypoints', 'serversion', 'iso8601time',
                  'session', 'serversionhint', 'docbaseconfig', 'serverconfg', 'readingmessages', 'knowncommands']

    def __init__(self, **kwargs):
        for attribute in DocbaseClient.attributes:
            setattr(self, attribute, kwargs.pop(attribute, None))
        super(DocbaseClient, self).__init__(**dict(kwargs, **{
            'version': NETWISE_VERSION,
            'release': NETWISE_RELEASE,
            'inumber': NETWISE_INUMBER,
        }))

        if self.serversion is None:
            self.serversion = 0
        if self.iso8601time is None:
            self.iso8601time = False
        if not self.session:
            self.session = NULL_ID
        if not self.docbaseid >= 0:
            self._resolve_docbase_id()
        if self.messages is None:
            self.messages = []
        if self.serversionhint is None:
            self.serversionhint = CLIENT_VERSION_ARRAY[3]

        self.readingmessages = False

        self._connect()
        self._fetch_entry_points()
        self._set_locale()

        if self.password and self.username:
            self.authenticate()

    def _resolve_docbase_id(self):
        response = self.request(Request,
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
        if m:
            self.docbaseid = int(m.group(1))
        self.disconnect()

    def _set_locale(self, charset=CHARSETS[DEFAULT_CHARSET]):
        if not charset in CHARSETS_REVERSE:
            raise RuntimeError("Unknown charset id %s" % charset)
        try:
            self.set_locale(charset)
        except Exception, e:
            if e.message.startswith('DM_SESSION_E_NO_TRANSLATOR'):
                if charset == CHARSETS[DEFAULT_CHARSET]:
                    raise e
                logging.warning("Unable to set charset %s, falling back to %s"
                                % (CHARSETS_REVERSE[charset], DEFAULT_CHARSET))
                self.set_locale(CHARSETS[DEFAULT_CHARSET])
            else:
                raise e

    def disconnect(self):
        try:
            if self.session and self.session != NULL_ID:
                self.request(Request,
                             type=RPC_CLOSE_SESSION,
                             data=[
                                 self.session,
                             ],
                             immediate=True,
                ).receive()
            super(DocbaseClient, self).disconnect()
        finally:
            self.session = None

    def _reconnect(self):
        ''

    def _connect(self):
        response = self.request(Request,
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
            self.serversion = DM_CLIENT_SERIALIZATION_VERSION_HINT
        else:
            self.serversion = 0

        if self.serversion == 0 or self.serversion == 1:
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

    def download(self, handle, rpc=RPC_GET_BLOCK5):
        i = 0
        while True:
            response = self.request(DownloadRequest, type=rpc, data=[handle, i], immediate=True).receive()
            length = response.next()
            last = response.next() == 1
            data = response.next()
            if length == 0:
                raise RuntimeError("Puller is closed")
            if length != len(data):
                raise RuntimeError("Invalid content size")
            yield data
            if last:
                break
            i += 1

    def rpc(self, rpc_id, data=None):
        if not data:
            data = []
        if self.session:
            if len(data) == 0 or data[0] != self.session:
                data.insert(0, self.session)

        (valid, o_data, collection, persistent, maybemore, records) = (None, None, None, None, None, None)

        response = self.request(Request, type=rpc_id, data=data, immediate=True).receive()
        message = response.next()
        o_data = response.last()
        if rpc_id == RPC_APPLY_FOR_OBJECT:
            valid = int(response.next()) > 0
            persistent = int(response.next()) > 0
        elif rpc_id == RPC_APPLY:
            collection = int(response.next())
            persistent = int(response.next()) > 0
            maybemore = int(response.next()) > 0
            valid = collection >= 0
        elif rpc_id == RPC_CLOSE_COLLECTION:
            pass
        elif rpc_id == RPC_GET_NEXT_PIECE:
            pass
        elif rpc_id == RPC_MULTI_NEXT:
            records = int(response.next())
            maybemore = int(response.next()) > 0
            valid = int(response.next()) > 0
        else:
            valid = int(response.next()) > 0

        if (o_data & 0x02 != 0) and not self.readingmessages:
            try:
                self.readingmessages = True
                self._get_messages()
            finally:
                self.readingmessages = False

        #TODO in some cases (e.g. AUTHENTICATE_USER) CS returns both OBDATA and RESULT
        if o_data & 0x02 != 0 and len(self.messages) > 0:
            reason = self._get_message(3)
            if len(reason) > 0:
                raise RuntimeError(reason)
        elif valid is not None and not valid:
            raise RuntimeError("Unknown error")
        elif len(self.messages) > 0:
            logging.debug(self._get_message(0))

        if o_data == 0x10 or (o_data == 0x01 and rpc_id == RPC_GET_NEXT_PIECE):
            message += self.rpc(RPC_GET_NEXT_PIECE).data

        return Response(data=message, odata=o_data, persistent=persistent, collection=collection, maybemore=maybemore,
                        records=records)

    def apply(self, rpc_id, object_id, method, request=None, cls=Collection):
        if rpc_id is None:
            rpc_id = RPC_APPLY

        if not object_id:
            object_id = NULL_ID

        response = self.rpc(rpc_id, [self._get_method(method), object_id, request])
        data = response.data

        if rpc_id == RPC_APPLY_FOR_STRING:
            return data
        elif rpc_id == RPC_APPLY_FOR_ID:
            return data
        elif rpc_id == RPC_APPLY_FOR_DOUBLE:
            return data
        elif rpc_id == RPC_APPLY_FOR_BOOL:
            return int(data) == 1
        elif rpc_id == RPC_APPLY_FOR_LONG:
            return data
        elif rpc_id == RPC_APPLY_FOR_TIME:
            return data

        if not cls:
            if rpc_id == RPC_APPLY:
                cls = Collection
            elif rpc_id == RPC_APPLY_FOR_OBJECT:
                cls = TypedObject

        if response.persistent:
            if cls == Collection:
                cls = PersistentCollection
            elif cls == TypedObject:
                cls = PersistentProxy

        result = cls(session=self, buffer=data)
        if response.collecton is not None and isinstance(result, Collection):
            result.collection = response.collection
            result.persistent = response.persistent
            result.records = response.records
            result.maybemore = response.maybemore
            if isinstance(request, TypedObject) and 'BATCH_HINT' in request:
                result.batchsize = request['BATCH_HINT']
            else:
                result.batchsize = DEFAULT_BATCH_SIZE

        return result

    def _get_messages(self):
        self.messages = [x for x in self.get_errors()]

    def _get_message(self, severity=0):
        if not self.messages:
            return ""
        message = ""
        for i in xrange(0, len(self.messages)):
            if self.messages[i]['SEVERITY'] < severity:
                continue
            if len(message) > 0:
                message += ", "
            message += self.messages[i]['NAME']
            if '1' in self.messages[i]:
                message += ": %s" % self.messages[i]['1']

        for i in xrange(len(self.messages) - 1, 1):
            if self.messages[i]['SEVERITY'] >= severity:
                self.messages.pop(i)

        return message

    def authenticate(self, username=None, password=None):
        if username and password:
            self.username = username
            self.password = password
        if not self.username:
            raise RuntimeError("Empty username")
        if not self.password:
            raise RuntimeError("Empty password")

        result = self.authenticate_user(self.username, self._obfuscate(self.password))
        if result['RETURN_VALUE'] != 1:
            raise RuntimeError("Unable to authenticate")

        self.docbaseconfig = self.get_docbase_config()
        self.serverconfig = self.get_server_config()

    def next_batch(self, collection, batch_hint=DEFAULT_BATCH_SIZE):
        return self.rpc(RPC_MULTI_NEXT, [collection, batch_hint])

    def close_collection(self, collection):
        self.rpc(RPC_CLOSE_COLLECTION, [collection])

    def _fetch_entry_points(self):
        if self.entrypoints is None:
            self.entrypoints = {
                'ENTRY_POINTS': 0,
                'GET_ERRORS': 558,
            }
            if self.knowncommands is None:
                self.knowncommands = {}
            Rpc.register_known_commands(self)
            for name in self.entrypoints.keys():
                self._add_entry_point(name)

        self.entrypoints = self.entry_points().methods()
        Rpc.register_known_commands(self)
        for name in self.entrypoints.keys():
            self._add_entry_point(name)

    def get_by_qualification(self, qualification):
        collection = self.query("select r_object_id from %s" % qualification)
        record = collection.next_record()
        if record:
            return self.fetch(record['r_object_id'])
        return None

    def get_type(self, name, vstamp=0):
        type_obj = get_type_from_cache(name)
        if type_obj:
            return type_obj
        data = None
        if "FETCH_TYPE" in self.entrypoints:
            data = self.fetch_type(name, vstamp)['result']
        else:
            data = self.rpc(RPC_FETCH_TYPE, [name]).data
        return TypeObject(session=self, buffer=data).type

    def query(self, query, for_update=False, batch_hint=DEFAULT_BATCH_SIZE, bof_dql=False):
        try:
            collection = self.execute(query, for_update, batch_hint, bof_dql)
        except Exception, e:
            raise RuntimeError("Error occurred while executing query: %s" % query, e)
        return collection

    def _obfuscate(self, password):
        if self._isobfuscated(password):
            return password
        return "".join(
            "%02x" % [x ^ 0xB6, 0xB6][x == 0xB6] for x in (ord(x) for x in password[::-1])
        )

    def _isobfuscated(self, password):
        if not re.match("^([0-9a-f]{2})+$", password):
            return False
        for x in re.findall("[0-9a-f]{2}", password):
            if int(x, 16) != 0xB6 and (int(x, 16) ^ 0xB6) > 127:
                return False
        return True

    def _get_method(self, name):
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

    def _add_entry_point(self, name):
        pep_name = Rpc.pep_name(name)
        if getattr(DocbaseClient, pep_name, None):
            return
        elif name in self.knowncommands:
            command = self.knowncommands[name]
            method = command.method
            cls = command.returntype
            request = getattr(Rpc, pep_name, None)
            needid = command.needid
            argc = 0
            if request:
                argc = request.func_code.co_argcount
            if needid:
                def inner(self, object_id=NULL_ID, *args):
                    if not request:
                        return method(self, object_id, name, None, cls)
                    if argc == 1:
                        return method(self, object_id, name, request(self), cls)
                    else:
                        return method(self, object_id, name, request(self, *args), cls)
            else:
                def inner(self, *args):
                    if not request:
                        return method(self, NULL_ID, name, None, cls)
                    if argc == 1:
                        return method(self, NULL_ID, name, request(self), cls)
                    else:
                        return method(self, NULL_ID, name, request(self, *args), cls)
        else:
            def inner(self, object_id=NULL_ID, request=None, cls=Collection):
                return Rpc.as_collection(self, object_id, name, request, cls)

        inner.__name__ = pep_name
        setattr(self.__class__, inner.__name__, inner)


class Response(object):
    attributes = ['data', 'odata', 'persistent', 'collection', 'records', 'maybemore']

    def __init__(self, **kwargs):
        for attribute in Response.attributes:
            setattr(self, attribute, kwargs.pop(attribute, None))

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


