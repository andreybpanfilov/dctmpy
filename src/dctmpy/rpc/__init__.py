#  Copyright (c) 2013 Andrey B. Panfilov <andrew@panfilov.tel>
#
#  See main module for license.
#

from dctmpy import *
from dctmpy.obj.collection import Collection, PersistentCollection
from dctmpy.obj.entrypoints import EntryPoints
from dctmpy.obj.persistent import PersistentProxy
from dctmpy.obj.typedobject import TypedObject
from dctmpy.rpc.rpccommands import Rpc


def pep_name(rpc_name):
    if 'EXEC' == rpc_name:
        return "execute"
    s = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', rpc_name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s).lower()


def register_known_commands(session):
    _register(session, Rpc('AclObjGetPermit', as_long, TypedObject, True))
    _register(session, Rpc('AclObjGetXPermit', as_long, TypedObject, True))
    _register(session, Rpc('ALLOW_BASE_AS_FEATURES', as_boolean, TypedObject))
    _register(session, Rpc('ALLOW_BASE_TYPE_CHANGES', as_boolean, TypedObject))
    _register(session, Rpc('AUDIT_ON', as_boolean, TypedObject))
    _register(session, Rpc('AUTHENTICATE_USER', as_object, TypedObject))
    _register(session, Rpc('CONVERT_ID', as_id, TypedObject, True))
    _register(session, Rpc('COUNT_SESSIONS', as_object, TypedObject))
    _register(session, Rpc('DB_STATS', as_object, TypedObject))
    _register(session, Rpc('DISABLE_TIMEOUT', as_boolean, TypedObject))
    _register(session, Rpc('DO_METHOD', as_object, TypedObject))
    _register(session, Rpc('DUMP_COUNTS', as_boolean, TypedObject))
    _register(session, Rpc('DUMP_CACHE', as_object, TypedObject))
    _register(session, Rpc('DUMP_JMS_CONFIG_LIST', as_object, TypedObject))
    _register(session, Rpc('ENABLE_TIMEOUT', as_boolean, TypedObject))
    _register(session, Rpc('ENCRYPT_PASSWORD', as_string, TypedObject))
    _register(session, Rpc('ENCRYPT_TEXT', as_string, TypedObject))
    _register(session, Rpc('END_PUSH_V2', as_object, TypedObject))
    _register(session, Rpc('ENTRY_POINTS', as_object, EntryPoints))
    _register(session, Rpc('EXEC', as_collection, Collection))
    _register(session, Rpc('EXEC_SELECT_SQL', as_collection, Collection))
    _register(session, Rpc('EXEC_SQL', as_boolean, Collection))
    _register(session, Rpc('FETCH', as_object, PersistentProxy, True))
    _register(session, Rpc('FETCH_TYPE', as_object, TypedObject))
    _register(session, Rpc('FTINDEX_AGENT_ADMIN', as_object, TypedObject))
    _register(session, Rpc('GET_ERRORS', as_collection, Collection))
    _register(session, Rpc('GET_DIST_CONTENT_MAP', as_object, TypedObject, True))
    _register(session, Rpc('GET_DOCBASE_CONFIG', as_object, TypedObject))
    _register(session, Rpc('GET_LAST_SQL', as_string, TypedObject))
    _register(session, Rpc('GET_LOGIN', as_string, TypedObject))
    _register(session, Rpc('GET_SERVER_CONFIG', as_object, TypedObject))
    _register(session, Rpc('GET_WORKFLOW_AGENT_STATUS', as_long, TypedObject))
    _register(session, Rpc('KILL_PULLER', as_boolean, TypedObject))
    _register(session, Rpc('LIST_SESSIONS', as_object, TypedObject))
    _register(session, Rpc('MAKE_PULLER', as_long, TypedObject))
    _register(session, Rpc('MAKE_PUSHER', as_long, TypedObject))
    _register(session, Rpc('SERVER_VERSION', as_string, TypedObject))
    _register(session, Rpc('SERVER_DIR', as_object, TypedObject))
    _register(session, Rpc('SET_LOCALE', as_boolean, TypedObject))
    _register(session, Rpc('SET_OPTIONS', as_boolean, TypedObject))
    _register(session, Rpc('SHOW_SESSIONS', as_collection, Collection))
    _register(session, Rpc('START_PUSH', as_boolean, TypedObject))
    _register(session, Rpc('TIME', as_time, TypedObject))
    _register(session, Rpc('SET_PUSH_OBJECT_STATUS', as_boolean, TypedObject))
    _register(session, Rpc('PUT_FILE', as_object, TypedObject))
    _register(session, Rpc('GET_TEMP_FILE', as_string, TypedObject))
    _register(session, Rpc('GET_FILE', as_string, TypedObject))
    _register(session, Rpc('NEXT_ID_LIST', as_next_id_list, TypedObject))
    _register(session, Rpc('CHECKOUT_LICENSE', as_object, TypedObject))
    _register(session, Rpc('SysObjSave', as_save_result, TypedObject, True))
    _register(session, Rpc('SAVE', as_save_result, TypedObject, True))
    _register(session, Rpc('SAVE_CONT_ATTRS', as_save_result, TypedObject, True))
    _register(session, Rpc('DQL_MATCH', as_collection, PersistentCollection))
    _register(session, Rpc('DUMP_COUNTS', as_object, TypedObject))
    _register(session, Rpc('FolderIdFindByPath', as_id, TypedObject))
    _register(session, Rpc('GETTYPE', as_string, TypedObject, True))
    _register(session, Rpc('GETTYPENAME', as_string, TypedObject, True))
    _register(session, Rpc('GET_LAST_SQL', as_string, TypedObject, True))
    _register(session, Rpc('GET_OBJECT_INFO', as_object, TypedObject))
    _register(session, Rpc('LOG_MESSAGE', as_boolean, TypedObject))
    _register(session, Rpc('STAMP_TRACE', as_boolean, TypedObject))
    _register(session, Rpc('LOG_OFF', as_boolean, TypedObject))
    _register(session, Rpc('LOG_ON', as_boolean, TypedObject))
    _register(session, Rpc('AUDIT_ON', as_boolean, TypedObject, True))
    _register(session, Rpc('AUDIT_SECURITY_FAILURE', as_boolean, TypedObject, True))
    _register(session, Rpc('AnyEvents', as_boolean, TypedObject))
    _register(session, Rpc('CLIENT_NAME', as_string, TypedObject))
    _register(session, Rpc('FIX_LINK_CNT', as_boolean, TypedObject, True))
    _register(session, Rpc('GET_ATTRIBUTE_DD_INFO', as_object, TypedObject))
    _register(session, Rpc('GET_ATTRIBUTE_NLS_INFO', as_object, TypedObject))
    _register(session, Rpc('GET_CONTENT_HASH', as_string, TypedObject, True))
    _register(session, Rpc('PROCESS_NEW_SERVER_MESSAGE', as_string, TypedObject, False))


def _register(session, command):
    if command.command in session.entrypoints:
        session.known_commands[command.command] = command


def as_long(session, object_id, method, request=None, cls=None):
    if not object_id:
        object_id = NULL_ID
    return session.apply(RPC_APPLY_FOR_LONG, object_id, method, request, cls)


def as_save_result(session, object_id, method, request=None, cls=None):
    return as_long(session, object_id, method, request, cls) == 1


def as_next_id_list(session, object_id, method, request=None, cls=None):
    return as_object(session, object_id, method, request, cls)['next_id']


def as_time(session, object_id, method, request=None, cls=None):
    if not object_id:
        object_id = NULL_ID
    return session.apply(RPC_APPLY_FOR_TIME, object_id, method, request, cls)


def as_boolean(session, object_id, method, request=None, cls=None):
    if not object_id:
        object_id = NULL_ID
    return session.apply(RPC_APPLY_FOR_BOOL, object_id, method, request, cls)


def as_id(session, object_id, method, request=None, cls=None):
    if not object_id:
        object_id = NULL_ID
    return session.apply(RPC_APPLY_FOR_ID, object_id, method, request, cls)


def as_string(session, object_id, method, request=None, cls=None):
    if not object_id:
        object_id = NULL_ID
    return session.apply(RPC_APPLY_FOR_STRING, object_id, method, request, cls)


def as_collection(session, object_id, method, request=None, cls=Collection):
    if not object_id:
        object_id = NULL_ID
    return session.apply(RPC_APPLY, object_id, method, request, cls)


def as_object(session, object_id, method, request=None, cls=TypedObject):
    if not object_id:
        object_id = NULL_ID
    return session.apply(RPC_APPLY_FOR_OBJECT, object_id, method, request, cls)
