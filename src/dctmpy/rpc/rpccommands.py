#  Copyright (c) 2013 Andrey B. Panfilov <andrew@panfilov.tel>
#
#  See main module for license.
#

from dctmpy.obj.collection import Collection
from dctmpy.obj.entrypoints import EntryPoints
from dctmpy.obj.persistent import PersistentProxy
from dctmpy.obj.typedobject import TypedObject
from dctmpy import *


class Rpc(object):
    attributes = ['command', 'method', 'returntype', 'needid']

    def __init__(self, command, method, returntype, needid):
        self.command = command
        self.method = method
        self.returntype = returntype
        self.needid = needid

    def __getattr__(self, name):
        if name in Rpc.attributes:
            return self.__getattribute__(ATTRIBUTE_PREFIX + name)
        else:
            return AttributeError("Unknown attribute %s in %s" % (name, str(self.__class__)))

    def __setattr__(self, name, value):
        if name in Rpc.attributes:
            Rpc.__setattr__(self, ATTRIBUTE_PREFIX + name, value)
        else:
            super(Rpc, self).__setattr__(name, value)

    @staticmethod
    def as_object(session, object_id, method, request=None, cls=TypedObject):
        if not object_id:
            object_id = NULL_ID
        return session.apply(RPC_APPLY_FOR_OBJECT, object_id, method, request, cls)

    @staticmethod
    def as_collection(session, object_id, method, request=None, cls=Collection):
        if not object_id:
            object_id = NULL_ID
        return session.apply(RPC_APPLY, object_id, method, request, cls)

    @staticmethod
    def as_string(session, object_id, method, request=None, cls=None):
        if not object_id:
            object_id = NULL_ID
        return session.apply(RPC_APPLY_FOR_STRING, object_id, method, request, cls)

    @staticmethod
    def as_id(session, object_id, method, request=None, cls=None):
        if not object_id:
            object_id = NULL_ID
        return session.apply(RPC_APPLY_FOR_ID, object_id, method, request, cls)

    @staticmethod
    def as_boolean(session, object_id, method, request=None, cls=None):
        if not object_id:
            object_id = NULL_ID
        return session.apply(RPC_APPLY_FOR_BOOL, object_id, method, request, cls)

    @staticmethod
    def as_time(session, object_id, method, request=None, cls=None):
        if not object_id:
            object_id = NULL_ID
        return session.apply(RPC_APPLY_FOR_TIME, object_id, method, request, cls)

    @staticmethod
    def as_long(session, object_id, method, request=None, cls=None):
        if not object_id:
            object_id = NULL_ID
        return session.apply(RPC_APPLY_FOR_LONG, object_id, method, request, cls)

    @staticmethod
    def _register(session, command):
        if command.command in session.entrypoints:
            session.knowncommands[command.command] = command

    @staticmethod
    def register_known_commands(session):
        Rpc._register(session, Rpc('GET_SERVER_CONFIG', Rpc.as_object, TypedObject, False))
        Rpc._register(session, Rpc('GET_DOCBASE_CONFIG', Rpc.as_object, TypedObject, False))
        Rpc._register(session, Rpc('ENTRY_POINTS', Rpc.as_object, EntryPoints, False))
        Rpc._register(session, Rpc('SET_LOCALE', Rpc.as_boolean, TypedObject, False))
        Rpc._register(session, Rpc('FETCH', Rpc.as_object, PersistentProxy, True))
        Rpc._register(session, Rpc('AUTHENTICATE_USER', Rpc.as_object, TypedObject, False))
        Rpc._register(session, Rpc('GET_ERRORS', Rpc.as_collection, Collection, False))
        Rpc._register(session, Rpc('FETCH_TYPE', Rpc.as_object, TypedObject, False))
        Rpc._register(session, Rpc('EXEC', Rpc.as_collection, Collection, False))
        Rpc._register(session, Rpc('TIME', Rpc.as_time, TypedObject, False))
        Rpc._register(session, Rpc('COUNT_SESSIONS', Rpc.as_object, TypedObject, False))
        Rpc._register(session, Rpc('EXEC_SELECT_SQL', Rpc.as_collection, Collection, False))
        Rpc._register(session, Rpc('FTINDEX_AGENT_ADMIN', Rpc.as_object, TypedObject, False))
        Rpc._register(session, Rpc('DUMP_JMS_CONFIG_LIST', Rpc.as_object, TypedObject, False))
        Rpc._register(session, Rpc('GET_LOGIN', Rpc.as_string, TypedObject, False))
        Rpc._register(session, Rpc('MAKE_PULLER', Rpc.as_long, TypedObject, False))
        Rpc._register(session, Rpc('KILL_PULLER', Rpc.as_boolean, TypedObject, False))
        Rpc._register(session, Rpc('GET_DIST_CONTENT_MAP', Rpc.as_object, TypedObject, True))
        Rpc._register(session, Rpc('CONVERT_ID', Rpc.as_id, TypedObject, True))
        Rpc._register(session, Rpc('MAKE_PUSHER', Rpc.as_long, TypedObject, False))
        Rpc._register(session, Rpc('START_PUSH', Rpc.as_boolean, TypedObject, False))
        Rpc._register(session, Rpc('END_PUSH_V2', Rpc.as_object, TypedObject, False))

    @staticmethod
    def set_locale(session, charset=get_charset_id()):
        obj = TypedObject(session=session)
        obj.add(AttrValue(name="LANGUAGE", type=INT, values=[get_locale_id()]))
        obj.add(AttrValue(name="CHARACTER_SET", type=INT, values=[charset]))
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
    def authenticate_user(session, username, password):
        obj = TypedObject(session=session)
        obj.add(AttrValue(name="CONNECT_POOLING", type=BOOL, values=[False]))
        obj.add(AttrValue(name="USER_PASSWORD", type=STRING, values=[password]))
        obj.add(AttrValue(name="AUTHENTICATION_ONLY", type=BOOL, values=[False]))
        obj.add(AttrValue(name="CHECK_ONLY", type=BOOL, values=[False]))
        obj.add(AttrValue(name="LOGON_NAME", type=STRING, values=[username]))
        return obj

    @staticmethod
    def get_server_config(session):
        obj = TypedObject(session=session)
        obj.add(AttrValue(name="OBJECT_TYPE", type=STRING, values=["dm_server_config"]))
        obj.add(AttrValue(name="FOR_REVERT", type=BOOL, values=[False]))
        obj.add(AttrValue(name="CACHE_VSTAMP", type=INT, values=[0]))
        return obj

    @staticmethod
    def get_docbase_config(session):
        obj = TypedObject(session=session)
        obj.add(AttrValue(name="OBJECT_TYPE", type=STRING, values=["dm_docbase_config"]))
        obj.add(AttrValue(name="FOR_REVERT", type=BOOL, values=[False]))
        obj.add(AttrValue(name="CACHE_VSTAMP", type=INT, values=[0]))
        return obj

    @staticmethod
    def fetch_type(session, typename, vstamp):
        obj = TypedObject(session=session)
        obj.add(AttrValue(name="TYPE_NAME", type=STRING, values=[typename]))
        obj.add(AttrValue(name="CACHE_VSTAMP", type=INT, values=[vstamp]))
        return obj

    @staticmethod
    def get_errors(session):
        obj = TypedObject(session=session)
        obj.add(AttrValue(name="OBJECT_TYPE", type=STRING, values=["dmError"]))
        return obj

    @staticmethod
    def execute(session, query, for_update, batch_hint, bof_dql=False):
        obj = TypedObject(session=session)
        obj.add(AttrValue(name="QUERY", type=STRING, values=[query]))
        obj.add(AttrValue(name="FOR_UPDATE", type=BOOL, values=[for_update]))
        obj.add(AttrValue(name="BATCH_HINT", type=INT, values=[batch_hint]))
        obj.add(AttrValue(name="BOF_DQL", type=BOOL, values=[bof_dql]))
        return obj

    @staticmethod
    def exec_select_sql(session, query, batch_hint):
        obj = TypedObject(session=session)
        obj.add(AttrValue(name="QUERY", type=STRING, values=[query]))
        obj.add(AttrValue(name="BATCH_HINT", type=INT, values=[batch_hint]))
        return obj

    @staticmethod
    def _folder_by_path_request(session, path):
        obj = TypedObject(session=session)
        obj.add(AttrValue(name="_FOLDER_PATH_", type=STRING, values=[path]))
        return obj

    @staticmethod
    def ftindex_agent_admin(session, indexname, agentname):
        obj = TypedObject(session=session)
        obj.add(AttrValue(name="NAME", type=STRING, values=[indexname]))
        obj.add(AttrValue(name="AGENT_INSTANCE_NAME", type=STRING, values=[agentname]))
        obj.add(AttrValue(name="ACTION", type=STRING, values=["status"]))
        return obj

    @staticmethod
    def get_login(session, username=None, scope="global", servername=None, timeout=300, singleuse=False):
        obj = TypedObject(session=session)
        if username:
            obj.add(AttrValue(name="OPTIONAL_USER_NAME", type=STRING, values=[username]))
        if scope:
            obj.add(AttrValue(name="LOGIN_TICKET_SCOPE", type=STRING, values=[scope]))
        if servername:
            obj.add(AttrValue(name="SERVER_NAME", type=STRING, values=[servername]))
        if timeout > 0:
            obj.add(AttrValue(name="LOGIN_TICKET_TIMEOUT", type=INT, values=[timeout]))
        obj.add(AttrValue(name="SINGLE_USE", type=BOOL, values=[singleuse]))
        return obj

    @staticmethod
    def make_puller(session, objectId, storeId, contentId, formatId, ticket, other=False, offline=False,
                    compression=False, noAccessUpdate=False):
        obj = TypedObject(session=session)
        obj.add(AttrValue(name="SYSOBJ_ID", type=ID, values=[objectId]))
        obj.add(AttrValue(name="STORE", type=ID, values=[storeId]))
        obj.add(AttrValue(name="CONTENT", type=ID, values=[contentId]))
        obj.add(AttrValue(name="FORMAT", type=ID, values=[formatId]))
        obj.add(AttrValue(name="TICKET", type=INT, values=[ticket]))
        obj.add(AttrValue(name="IS_OTHER", type=BOOL, values=[other]))
        obj.add(AttrValue(name="IS_OFFLINE", type=BOOL, values=[offline]))
        obj.add(AttrValue(name="COMPRESSION", type=BOOL, values=[compression]))
        if noAccessUpdate:
            obj.add(AttrValue(name="NO_ACCESS_UPDATE", type=BOOL, values=[noAccessUpdate]))
        return obj

    @staticmethod
    def kill_puller(session, handle):
        obj = TypedObject(session=session)
        obj.add(AttrValue(name="HANDLE", type=INT, values=[handle]))
        return obj

    @staticmethod
    def get_dist_content_map(session, fmt=None, page_number=0, page_modifier='', netloc_id='',
                             request_time=int(time.mktime(time.gmtime())),
                             expire_delta=360, lookup_resourcefork_info=False, include_surrogate_get=True):
        obj = TypedObject(session=session)
        if fmt:
            obj.add(AttrValue(name="format", type=STRING, values=[fmt]))
        obj.add(AttrValue(name="page_number", type=INT, values=[page_number]))
        obj.add(AttrValue(name="page_modifier", type=STRING, values=[page_modifier]))
        obj.add(AttrValue(name="netloc_id", type=STRING, values=[netloc_id]))
        obj.add(AttrValue(name="request_time", type=STRING, values=[str(request_time)]))
        obj.add(AttrValue(name="expire_delta", type=STRING, values=[str(expire_delta)]))
        obj.add(AttrValue(name="lookup_resourcefork_info", type=BOOL, values=[lookup_resourcefork_info]))
        obj.add(AttrValue(name="include_surrogate_get", type=BOOL, values=[include_surrogate_get]))
        return obj

    @staticmethod
    def convert_id(session, fmt, page=0, page_modifier='', convert=False, useconvert=False):
        obj = TypedObject(session=session)
        obj.add(AttrValue(name="page", type=INT, values=[page]))
        obj.add(AttrValue(name="format", type=STRING, values=[fmt]))
        obj.add(AttrValue(name="page_modifier", type=STRING, values=[page_modifier]))
        obj.add(AttrValue(name="convert", type=BOOL, values=[convert]))
        obj.add(AttrValue(name="useconvert", type=BOOL, values=[useconvert]))
        return obj

    @staticmethod
    def make_pusher(session, store, compression=False):
        obj = TypedObject(session=session)
        obj.add(AttrValue(name="STORE", type=ID, values=[store]))
        obj.add(AttrValue(name="COMPRESSION", type=BOOL, values=[compression]))
        return obj

    @staticmethod
    def start_push(session, handle, content_id, fmt, size, size_low, size_high=0, d_ticket=0, is_other=False,
                   compression=False, can_use_new_callbacks=True, encoded_content_attrs='', i_partition=0):
        obj = TypedObject(session=session)
        obj.add(AttrValue(name="HANDLE", type=INT, values=[handle]))
        obj.add(AttrValue(name="CONTENT_ID", type=ID, values=[content_id]))
        obj.add(AttrValue(name="FORMAT", type=ID, values=[fmt]))
        obj.add(AttrValue(name="D_TICKET", type=INT, values=[d_ticket]))
        obj.add(AttrValue(name="SIZE", type=INT, values=[size]))
        obj.add(AttrValue(name="SIZE_LOW", type=INT, values=[size_low]))
        obj.add(AttrValue(name="SIZE_HIGH", type=INT, values=[size_high]))
        obj.add(AttrValue(name="IS_OTHER", type=BOOL, values=[is_other]))
        obj.add(AttrValue(name="COMPRESSION", type=BOOL, values=[compression]))
        obj.add(AttrValue(name="CAN_USE_NEW_CALLBACKS", type=BOOL, values=[can_use_new_callbacks]))
        obj.add(AttrValue(name="ENCODED_CONTENT_ATTRS", type=STRING, values=[encoded_content_attrs]))
        obj.add(AttrValue(name="I_PARTITION", type=INT, values=[i_partition]))
        return obj

    @staticmethod
    def end_push_v2(session, handle):
        obj = TypedObject(session=session)
        obj.add(AttrValue(name="HANDLE", type=INT, values=[handle]))
        return obj

    @staticmethod
    def pep_name(rpc_name):
        if 'EXEC' == rpc_name:
            return "execute"
        s = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', rpc_name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s).lower()
