# Copyright (c) 2013 Andrey B. Panfilov <andrew@panfilov.tel>
#
# See main module for license.
#
import logging

from dctmpy import *
from dctmpy.net.netwise import Netwise
from dctmpy.net.request import Request, DownloadRequest, UploadRequest
from dctmpy.obj.collection import Collection, PersistentCollection
from dctmpy.obj.persistent import PersistentProxy
from dctmpy.obj.type import TypeObject
from dctmpy.obj.typedobject import TypedObject
from dctmpy.rpc import pep_name, register_known_commands, as_collection
from dctmpy.rpc.messages import get_message, ERROR, INFORMATION
from dctmpy.rpc.rpccommands import Rpc

NETWISE_VERSION = 3
NETWISE_RELEASE = 5
NETWISE_INUMBER = 769

DEFAULT_CHARSET = 'UTF-8'

MAX_REQUEST_LEN = CHUNKS[RPC_GET_BLOCK5]


class DocbaseClient(Netwise):
    attributes = ['docbaseid', 'username', 'password', 'messages', 'entrypoints',
                  'ser_version', 'iso8601time', 'session', 'ser_version_hint',
                  'docbaseconfig', 'serverconfg', 'known_commands', 'reading_messages',
                  'collections', 'identity']

    def __init__(self, **kwargs):
        for attribute in DocbaseClient.attributes:
            setattr(self, attribute, kwargs.pop(attribute, None))
        super(DocbaseClient, self).__init__(**dict(kwargs, **{
            'version': NETWISE_VERSION,
            'release': NETWISE_RELEASE,
            'inumber': NETWISE_INUMBER,
        }))

        self.collections = dict()
        self.reading_messages = False

        if self.ser_version is None:
            self.ser_version = 0
        if self.iso8601time is None:
            self.iso8601time = False
        if not self.docbaseid >= 0:
            self._resolve_docbase_id()
        if self.messages is None:
            self.messages = []
        if self.ser_version_hint is None:
            self.ser_version_hint = CLIENT_VERSION_ARRAY[3]

        self._connect()
        self._fetch_entry_points()
        self._set_locale()

        if self._can_authenticate():
            self.authenticate()

    def _resolve_docbase_id(self):
        data = [-1, EMPTY_STRING, CLIENT_VERSION_STRING,
                EMPTY_STRING, CLIENT_VERSION_ARRAY, NULL_ID, ]
        response = self.request(Request, type=RPC_NEW_SESSION_BY_ADDR, data=data)
        reason = response.next()
        m = re.search('Wrong docbase id: \(-1\) expecting: \((\d+)\)', reason)
        if m:
            self.docbaseid = int(m.group(1))
        self.disconnect()

    def _set_locale(self, charset=CHARSETS[DEFAULT_CHARSET]):
        if charset not in CHARSETS_REVERSE:
            raise RuntimeError("Unknown charset id %s" % charset)
        try:
            self.set_locale(charset)
        except Exception, e:
            if not e.message.startswith('[DM_SESSION_E_NO_TRANSLATOR]'):
                raise e
            if charset == CHARSETS[DEFAULT_CHARSET]:
                raise e
            logging.warning("Unable to set charset %s, falling back to %s"
                            % (CHARSETS_REVERSE[charset], DEFAULT_CHARSET))
            self.set_locale(CHARSETS[DEFAULT_CHARSET])

    def disconnect(self):
        for collection in self.collections.values():
            collection.close()
        self._disconnect()

    def _reconnect(self):
        pass

    def _disconnect(self):
        if not self.session:
            return
        if self.session == NULL_ID:
            return
        try:
            if self.session and self.session != NULL_ID:
                self.request(Request, type=RPC_CLOSE_SESSION)
            super(DocbaseClient, self).disconnect()
        finally:
            self.session = None

    def _connect(self):
        data = [self.docbaseid, EMPTY_STRING, CLIENT_VERSION_STRING,
                EMPTY_STRING, CLIENT_VERSION_ARRAY, NULL_ID, ]
        response = self.request(Request, type=RPC_NEW_SESSION_BY_ADDR, data=data)

        reason = response.next()
        server_version = response.next()
        if server_version[7] == DM_CLIENT_SERIALIZATION_VERSION_HINT:
            self.ser_version = DM_CLIENT_SERIALIZATION_VERSION_HINT
        else:
            self.ser_version = 0

        if self.ser_version == 0 or self.ser_version == 1:
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
            response = self.request(DownloadRequest, False, type=rpc, data=[handle, i])
            length = response.next()
            last = response.next() == 1
            data = response.next()
            if length == 0 and not last:
                raise RuntimeError("Puller is closed")
            if isinstance(data, list):
                l = 0
                for chunk in data:
                    l += len(chunk)
                if length != l:
                    raise RuntimeError("Invalid content size")
                for chunk in data:
                    yield chunk
            else:
                if length != len(data):
                    raise RuntimeError("Invalid content size")
                yield data
            if last:
                break
            i += 1

    def upload(self, handle, data):
        offset = 0
        response = self.request(UploadRequest, type=RPC_DO_PUSH, data=[handle])
        while True:
            stop = response.rpc == 17023
            (chunk, offset, last) = create_chunk(data, offset, response.rpc)
            cls = [UploadRequest, Request][stop]
            sequence = [response.sequence, self.sequence][stop]
            send = [[len(chunk), [0, 1][last], chunk], []][stop]
            response = self.request(cls, False, type=0, data=send, sequence=sequence)
            if stop:
                break

    def rpc(self, rpc_id, data=None):
        if not data:
            data = []

        (valid, oob_data, collection, persistent, may_be_more, record_count) = (None, None, None, None, None, None)

        response = self.request(Request, type=rpc_id, data=data)
        message = response.next()
        if rpc_id == RPC_APPLY_FOR_OBJECT:
            valid = int(response.next()) > 0
            persistent = int(response.next()) > 0
        elif rpc_id == RPC_APPLY:
            collection = int(response.next())
            persistent = int(response.next()) > 0
            may_be_more = int(response.next()) > 0
            valid = collection >= 0
        elif rpc_id == RPC_CLOSE_COLLECTION:
            self.collections.pop(data[1], None)
        elif rpc_id == RPC_GET_NEXT_PIECE:
            pass
        elif rpc_id == RPC_MULTI_NEXT:
            record_count = int(response.next())
            may_be_more = int(response.next()) > 0
            valid = int(response.next()) > 0
        else:
            valid = int(response.next()) > 0
        oob_data = response.next()

        has_messages = oob_data & 0x02 != 0

        if has_messages:
            self._get_messages()

        # TODO in some cases (e.g. AUTHENTICATE_USER) CS returns both OOBDATA and RESULT
        if has_messages and len(self.messages) > 0:
            reason = self._get_message(ERROR)
            self._log_messages()
            if reason:
                raise RuntimeError(reason)
        elif valid is not None and not valid:
            raise RuntimeError("Unknown error")

        if oob_data == 0x10 or (oob_data == 0x01 and rpc_id == RPC_GET_NEXT_PIECE):
            message += self.rpc(RPC_GET_NEXT_PIECE).data

        return Response(data=message, oob_data=oob_data, persistent=persistent,
                        collection=collection, may_be_more=may_be_more,
                        record_count=record_count)

    def apply_chunks(self, rpc_id, object_id, method, request, cls=Collection):
        if not object_id or object_id == NULL_ID:
            object_id = self.session
        self.set_push_object_status(object_id, True)
        for part in chunks(request, MAX_REQUEST_LEN):
            self.apply(RPC_APPLY_FOR_LONG, object_id, method, part)
        self.set_push_object_status(object_id, False)
        return self.apply(rpc_id, object_id, method, "_USE_SESSION_CHUNKED_OBJ_STRING_", cls)

    def apply(self, rpc_id, object_id, method, request=None, cls=Collection):
        if rpc_id is None:
            rpc_id = RPC_APPLY

        if not object_id:
            object_id = NULL_ID

        req = request
        if object_id != NULL_ID and req is None:
            req = TypedObject(session=self)
        if isinstance(req, TypedObject):
            req = req.serialize()
        if req and len(req) > MAX_REQUEST_LEN:
            return self.apply_chunks(rpc_id, object_id, method, req, cls)

        response = self.rpc(rpc_id, [self._get_method(method), object_id, req])
        data = response.data

        if rpc_id == RPC_APPLY_FOR_STRING:
            return str(data)
        elif rpc_id == RPC_APPLY_FOR_ID:
            return str(data)
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

        if is_empty(data):
            return None

        result = cls(session=self, buffer=data)
        if response.collection is not None and isinstance(result, Collection):
            result.collection = response.collection
            result.persistent = response.persistent
            result.record_count = response.record_count
            result.may_be_more = response.may_be_more
            if isinstance(request, TypedObject) and 'BATCH_HINT' in request:
                result.batch_size = request['BATCH_HINT']
            else:
                result.batch_size = DEFAULT_BATCH_SIZE

        if isinstance(result, Collection):
            self.collections[result.collection] = result

        return result

    def _get_messages(self):
        if self.reading_messages:
            return
        try:
            self.reading_messages = True
            for message in self.get_errors():
                self.messages.append(message)
        finally:
            self.reading_messages = False

    def _log_messages(self):
        message = self._get_message(INFORMATION)
        if message:
            logging.debug(message)

    def _get_message(self, severity=INFORMATION):
        if self.reading_messages or len(self.messages) == 0:
            return None
        try:
            self.reading_messages = True
            messages = []
            for i in xrange(len(self.messages) - 1, -1, -1):
                message = self.messages[i]
                if message['SEVERITY'] < severity:
                    continue
                messages.append(self.messages.pop(i))
            result = ""
            for message in messages:
                local = self._format_message(message)
                if len(result) > 0:
                    result += "\n"
                result += local
            return result
        finally:
            self.reading_messages = False

    def _format_message(self, message):
        template = get_message(message)
        args = []
        for i in xrange(1, message['COUNT'] + 1):
            args.append(message[str(i)])
        if template:
            try:
                return template.format(*args)
            except:
                pass
        return self.process_new_server_message(message)

    def _can_authenticate(self):
        if not self.username:
            return False
        if self.identity and self.identity.trusted:
            return True
        if not self.password:
            return False
        return True

    def authenticate(self, username=None, password=None, identity=None):
        if username:
            self.username = username
        if password:
            self.password = password
        if identity:
            self.identity = identity

        if not self._can_authenticate():
            raise RuntimeError("Can't perform authentication")

        result = self.authenticate_user(self.username, self.obfuscate(self.password), self.identity)
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
            if self.known_commands is None:
                self.known_commands = {}
            register_known_commands(self)
            for name in self.entrypoints.keys():
                self._add_entry_point(name)

        self.entrypoints = self.entry_points().methods()
        register_known_commands(self)
        for name in self.entrypoints.keys():
            self._add_entry_point(name)

    def get_by_qualification(self, qualification):
        collection = self.query("select r_object_id from %s" % qualification)
        try:
            record = collection.next_record()
            if record:
                return self.get_object(record['r_object_id'])
            return None
        finally:
            if collection:
                collection.close()

    def get_object(self, objectid):
        obj = self.fetch(objectid)
        if obj is None:
            raise RuntimeError("Unable to fetch object with id %s" % objectid)
        return obj

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

    def next_id(self, tag):
        return self.next_id_list(tag, 1)[0]

    def obfuscate(self, password):
        if not password:
            return None
        if self._isobfuscated(password):
            return password
        return "".join(
            "%02x" % [x ^ 0xB6, 0xB6][x == 0xB6]
            for x in (ord(x) for x in password[::-1])
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

    def _add_entry_point(self, name):
        func = pep_name(name)
        if getattr(DocbaseClient, func, None):
            return
        elif name in self.known_commands:
            command = self.known_commands[name]
            method = command.method
            cls = command.return_type
            request = getattr(Rpc, func, None)
            need_id = command.need_id
            argc = 0
            if request:
                argc = request.func_code.co_argcount
            if need_id:
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
                return as_collection(self, object_id, name, request, cls)

        inner.__name__ = func
        setattr(self.__class__, inner.__name__, inner)

    def request(self, cls, add_session=True, **kwargs):
        data = kwargs.pop("data", [])
        if add_session and self.session:
            if len(data) == 0 or data[0] != self.session:
                data.insert(0, self.session)
        kwargs["data"] = data
        return super(DocbaseClient, self).request(cls, **kwargs)


class Response(object):
    attributes = ['data', 'oob_data', 'persistent', 'collection', 'record_count', 'may_be_more']

    def __init__(self, **kwargs):
        for attribute in Response.attributes:
            setattr(self, attribute, kwargs.pop(attribute, None))
