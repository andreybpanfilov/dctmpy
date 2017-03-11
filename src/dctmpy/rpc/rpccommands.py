# Copyright (c) 2013 Andrey B. Panfilov <andrew@panfilov.tel>
#
# See main module for license.
#
from dctmpy import *
from dctmpy.obj.typedobject import TypedObject


class Rpc(object):
    attributes = ['command', 'method', 'return_type', 'need_id']

    def __init__(self, command, method, return_type, need_id=False):
        self.command = command
        self.method = method
        self.return_type = return_type
        self.need_id = need_id

    @staticmethod
    def set_locale(session, charset=get_charset_id()):
        obj = TypedObject(session=session)
        obj.set_int("LANGUAGE", get_locale_id())
        obj.set_int("CHARACTER_SET", charset)
        obj.set_int("PLATFORM_ENUM", get_platform_id())
        obj.set_string("PLATFORM_VERSION_IMAGE", "python")
        obj.set_int("UTC_OFFSET", get_offset_in_seconds())
        obj.set_int("SDF_AN_custom_date_order", 0)
        obj.set_int("SDF_AN_custom_scan_fields", 0)
        obj.set_string("SDF_AN_date_separator", "/")
        obj.set_int("SDF_AN_date_order", 2)
        obj.set_bool("SDF_AN_day_leading_zero", True)
        obj.set_bool("SDF_AN_month_leading_zero", True)
        obj.set_bool("SDF_AN_century", True)
        obj.set_string("SDF_AN_time_separator", ":")
        obj.set_bool("SDF_AN_hours_24", True)
        obj.set_bool("SDF_AN_hour_leading_zero", True)
        obj.set_bool("SDF_AN_noon_is_zero", False)
        obj.set_string("SDF_AN_am", "AM")
        obj.set_string("SDF_AN_pm", "PM")
        obj.append_int("PLATFORM_EXTRA", [0, 0, 0, 0])
        obj.set_string("APPLICATION_CODE", "")
        return obj

    @staticmethod
    def authenticate_user(session, username, password, identity=None):
        obj = TypedObject(session=session)
        obj.set_bool("CONNECT_POOLING", False)
        obj.set_string("USER_PASSWORD", password)
        obj.set_bool("AUTHENTICATION_ONLY", False)
        obj.set_bool("CHECK_ONLY", False)
        obj.set_string("LOGON_NAME", username)
        if identity:
            if identity.trusted:
                obj.set_bool("TRUSTED_LOGIN_ALLOWED", True)
                obj.set_string("OS_LOGON_NAME", username)
            auth_data = identity.get_auth_data()
            if auth_data:
                obj.set_string("CLIENT_AUTH_DATA", auth_data)
        return obj

    @staticmethod
    def get_server_config(session):
        obj = TypedObject(session=session)
        obj.set_string("OBJECT_TYPE", "dm_server_config")
        obj.set_bool("FOR_REVERT", False)
        obj.set_int("CACHE_VSTAMP", 0)
        return obj

    @staticmethod
    def get_docbase_config(session):
        obj = TypedObject(session=session)
        obj.set_string("OBJECT_TYPE", "dm_docbase_config")
        obj.set_bool("FOR_REVERT", False)
        obj.set_int("CACHE_VSTAMP", 0)
        return obj

    @staticmethod
    def fetch_type(session, typename, vstamp):
        obj = TypedObject(session=session)
        obj.set_string("TYPE_NAME", typename)
        obj.set_int("CACHE_VSTAMP", vstamp)
        return obj

    @staticmethod
    def get_errors(session):
        obj = TypedObject(session=session)
        obj.set_string("OBJECT_TYPE", "dmError")
        return obj

    @staticmethod
    def execute(session, query, for_update, batch_hint, bof_dql=False):
        obj = TypedObject(session=session)
        obj.set_string("QUERY", query)
        obj.set_bool("FOR_UPDATE", for_update)
        obj.set_int("BATCH_HINT", batch_hint)
        obj.set_bool("BOF_DQL", bof_dql)
        return obj

    @staticmethod
    def exec_select_sql(session, query, batch_hint=50):
        obj = TypedObject(session=session)
        obj.set_string("QUERY", query)
        obj.set_int("BATCH_HINT", batch_hint)
        return obj

    @staticmethod
    def exec_sql(session, query):
        obj = TypedObject(session=session)
        obj.set_string("QUERY", query)
        return obj

    @staticmethod
    def _folder_by_path_request(session, path):
        obj = TypedObject(session=session)
        obj.set_string("_FOLDER_PATH_", path)
        return obj

    @staticmethod
    def ftindex_agent_admin(session, indexname, agentname):
        obj = TypedObject(session=session)
        obj.set_string("NAME", indexname)
        obj.set_string("AGENT_INSTANCE_NAME", agentname)
        obj.set_string("ACTION", "status")
        return obj

    @staticmethod
    def get_login(session, username=None, scope="global", servername=None, timeout=300, singleuse=False):
        obj = TypedObject(session=session)
        if username:
            obj.set_string("OPTIONAL_USER_NAME", username)
            if scope:
                obj.set_string("LOGIN_TICKET_SCOPE", scope)
            if servername:
                obj.set_string("SERVER_NAME", servername)
            if timeout > 0:
                obj.set_int("LOGIN_TICKET_TIMEOUT", timeout)
            obj.set_bool("SINGLE_USE", singleuse)
            return obj

    @staticmethod
    def make_puller(session, objectId, storeId, contentId, formatId, ticket, other=False, offline=False,
                    compression=False, noAccessUpdate=False):
        obj = TypedObject(session=session)
        obj.set_id("SYSOBJ_ID", objectId)
        obj.set_id("STORE", storeId)
        obj.set_id("CONTENT", contentId)
        obj.set_id("FORMAT", formatId)
        obj.set_int("TICKET", ticket)
        obj.set_bool("IS_OTHER", other)
        obj.set_bool("IS_OFFLINE", offline)
        obj.set_bool("COMPRESSION", compression)
        if noAccessUpdate:
            obj.set_bool("NO_ACCESS_UPDATE", noAccessUpdate)
        return obj

    @staticmethod
    def kill_puller(session, handle):
        obj = TypedObject(session=session)
        obj.set_int("HANDLE", handle)
        return obj

    @staticmethod
    def get_dist_content_map(session, fmt=None, page_number=0, page_modifier='', netloc_id='',
                             request_time=int(time.mktime(time.gmtime())),
                             expire_delta=360, lookup_resourcefork_info=False, include_surrogate_get=True):
        obj = TypedObject(session=session)
        if fmt:
            obj.set_string("format", fmt)
            obj.set_int("page_number", page_number)
            obj.set_string("page_modifier", page_modifier)
            obj.set_string("netloc_id", netloc_id)
            obj.set_string("request_time", str(request_time))
            obj.set_string("expire_delta", str(expire_delta))
            obj.set_bool("lookup_resourcefork_info", lookup_resourcefork_info)
            obj.set_bool("include_surrogate_get", include_surrogate_get)
            return obj

    @staticmethod
    def convert_id(session, fmt, page=0, page_modifier='', convert=False, useconvert=False):
        obj = TypedObject(session=session)
        obj.set_int("page", page)
        obj.set_string("format", fmt)
        obj.set_string("page_modifier", page_modifier)
        obj.set_bool("convert", convert)
        obj.set_bool("useconvert", useconvert)
        return obj

    @staticmethod
    def make_pusher(session, store, compression=False):
        obj = TypedObject(session=session)
        obj.set_id("STORE", store)
        obj.set_bool("COMPRESSION", compression)
        return obj

    @staticmethod
    def start_push(session, handle, content_id, fmt, size, d_ticket=0, is_other=False,
                   compression=False, can_use_new_callbacks=True, encoded_content_attrs='', i_partition=0):
        obj = TypedObject(session=session)
        obj.set_int("HANDLE", handle)
        obj.set_id("CONTENT_ID", content_id)
        obj.set_id("FORMAT", fmt)
        obj.set_int("D_TICKET", d_ticket)
        obj.set_int("SIZE", size & 0xFFFFFFFF)
        obj.set_int("SIZE_LOW", size & 0xFFFFFFFF)
        obj.set_int("SIZE_HIGH", size >> 32)
        obj.set_bool("IS_OTHER", is_other)
        obj.set_bool("COMPRESSION", compression)
        obj.set_bool("CAN_USE_NEW_CALLBACKS", can_use_new_callbacks)
        obj.set_string("ENCODED_CONTENT_ATTRS", encoded_content_attrs)
        obj.set_int("I_PARTITION", i_partition)
        return obj

    @staticmethod
    def end_push_v2(session, handle):
        obj = TypedObject(session=session)
        obj.set_int("HANDLE", handle)
        return obj

    @staticmethod
    def do_method(session, method, arguments=None, async=None, direct=None, save=None, timeout=None,
                  as_server=None,
                  trace=None):
        obj = TypedObject(session=session)
        obj.set_string("METHOD", method)
        if arguments is not None:
            obj.set_string("ARGUMENTS", arguments)
        if timeout is not None:
            obj.set_int("TIME_OUT", timeout)
        if direct is not None:
            obj.set_bool("LAUNCH_DIRECT", direct)
        if async is not None:
            obj.set_bool("LAUNCH_ASYNC", async)
        if save is not None:
            obj.set_bool("SAVE_RESULTS", save)
        if as_server is not None:
            obj.set_bool("RUN_AS_SERVER", as_server)
        if trace is not None:
            obj.set_bool("TRACE_LAUNCH", trace)
        return obj

    @staticmethod
    def encrypt_password(session, password):
        obj = TypedObject(session=session)
        obj.set_string("PASSWORD_TO_ENCRYPT", session.obfuscate(password))
        return obj

    @staticmethod
    def encrypt_text(session, password):
        obj = TypedObject(session=session)
        obj.set_string("TEXT_TO_ENCRYPT", session.obfuscate(password))
        return obj

    @staticmethod
    def allow_base_as_features(session, allow=True):
        obj = TypedObject(session=session)
        obj.set_bool("VALUE", allow)
        return obj

    @staticmethod
    def allow_base_type_changes(session, allow=True):
        obj = TypedObject(session=session)
        obj.set_bool("ALLOW_CHANGE_FLAG", allow)
        return obj

    @staticmethod
    def audit_on(session, event):
        obj = TypedObject(session=session)
        obj.set_string("EVENT", event)
        return obj

    @staticmethod
    def acl_obj_get_permit(session, accessor):
        obj = TypedObject(session=session)
        if not is_empty(accessor):
            obj.set_string("_ACC_NAME_", accessor)
            return obj

    @staticmethod
    def acl_obj_get_x_permit(session, accessor):
        obj = TypedObject(session=session)
        if not is_empty(accessor):
            obj.set_string("_ACC_NAME_", accessor)
            return obj

    @staticmethod
    def dump_cache(session, tag):
        obj = TypedObject(session=session)
        obj.set_int("TAG", tag)
        return obj

    @staticmethod
    def server_dir(session, path, dirs=True, files=True, links=True):
        obj = TypedObject(session=session)
        obj.set_string("DIRECTORY", path)
        obj.set_bool("LIST_DIR", dirs)
        obj.set_bool("LIST_FILE", files)
        obj.set_bool("LIST_LINK", links)
        return obj

    @staticmethod
    def set_options(session, option, value=True):
        obj = TypedObject(session=session)
        obj.set_string("OPTION", option)
        obj.set_bool("VALUE", value)
        return obj

    @staticmethod
    def set_push_object_status(session, object_id, value):
        obj = TypedObject(session=session)
        obj.set_id("_PUSHED_ID_", object_id)
        obj.set_bool("_PUSH_STATUS_", value)
        return obj

    @staticmethod
    def get_temp_file(session):
        obj = TypedObject(session=session)
        return obj

    @staticmethod
    def put_file(session, storage_id, file, format):
        obj = TypedObject(session=session)
        obj.set_id("STORAGE", storage_id)
        obj.set_string("FILE", file)
        obj.set_id("FORMAT", format)
        obj.set_bool("MAC_CLIENT", False)
        return obj

    @staticmethod
    def get_file(session, content_id, file_name=None):
        content = session.get_object(content_id)
        obj = TypedObject(session=session)
        obj.set_id("STORAGE", content['storage_id'])
        obj.set_id("FORMAT", content['format'])
        obj.set_id("CONTENT", content['r_object_id'])
        obj.set_int("D_TICKET", content['data_ticket'])
        obj.set_bool("MAC_CLIENT", False)
        if file_name:
            obj.set_string("OBJNAME", file_name)
        return obj

    @staticmethod
    def next_id_list(session, tag, how_many=10):
        obj = TypedObject(session=session)
        obj.set_int("TAG", tag)
        obj.set_int("HOW_MANY", how_many)
        return obj

    @staticmethod
    def checkout_license(session, feature_name, feature_version, user=None):
        obj = TypedObject(session=session)
        if not user:
            user = session.username
        obj.set_string("FEATURE_NAME", feature_name)
        obj.set_string("FEATURE_VERSION", feature_version)
        obj.set_string("USER_LOGIN_NAME", user)
        return obj

    @staticmethod
    def save(session, object):
        return object

    @staticmethod
    def sys_obj_save(session, object):
        return object

    @staticmethod
    def save_cont_attrs(session, object):
        return object

    @staticmethod
    def dql_match(session, type, predicate):
        obj = TypedObject(session=session)
        obj.set_string("QUERY_TYPE", type)
        obj.set_string("QUERY_PREDICATE", predicate)
        return obj

    @staticmethod
    def folder_id_find_by_path(session, path):
        obj = TypedObject(session=session)
        obj.set_string("_FOLDER_PATH_", path)
        return obj

    @staticmethod
    def get_object_info(session, object_id, fetch_immutability_status=False):
        obj = TypedObject(session=session)
        obj.set_id("OBJECT_ID", object_id)
        obj.set_bool("FETCH_IMMUTABILITY_STATUS", fetch_immutability_status)
        return obj

    @staticmethod
    def log_message(session, message):
        obj = TypedObject(session=session)
        obj.set_id("MESSAGE", message)
        return obj

    @staticmethod
    def stamp_trace(session, message):
        obj = TypedObject(session=session)
        obj.set_id("MESSAGE", message)
        return obj

    @staticmethod
    def log_on(session, detail):
        obj = TypedObject(session=session)
        obj.set_bool("DETAIL", detail)
        return obj

    @staticmethod
    def audit_on(session, event):
        obj = TypedObject(session=session)
        obj.set_string("EVENT", event)
        return obj

    @staticmethod
    def audit_security_failure(session, op_name, message=None):
        obj = TypedObject(session=session)
        obj.set_string("OPERATION_NAME", op_name)
        if message:
            obj.set_string("ERROR_MESSAGE", message)
        return obj

    @staticmethod
    def get_attribute_dd_info(session, type, attribute, policy=NULL_ID, state=0):
        obj = TypedObject(session=session)
        obj.set_string("TYPE_NAME", type)
        obj.set_string("ATTR_NAME", attribute)
        obj.set_id("POLICY_ID", policy)
        obj.set_int("POLICY_STATE", state)
        return obj

    @staticmethod
    def get_attribute_nls_info(session, type, attribute, policy=NULL_ID, state=0):
        obj = TypedObject(session=session)
        obj.set_string("TYPE_NAME", type)
        obj.set_string("ATTR_NAME", attribute)
        obj.set_id("POLICY_ID", policy)
        obj.set_int("POLICY_STATE", state)
        return obj

    @staticmethod
    def process_new_server_message(session, message):
        return message
