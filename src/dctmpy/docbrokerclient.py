#  Copyright (c) 2013 Andrey B. Panfilov <andrew@panfilov.tel>
#
#  See main module for license.
#
from dctmpy import parse_address
from dctmpy.net.netwise import Netwise
from dctmpy.net.request import Request
from dctmpy.obj.docbroker import DocbaseMap
from dctmpy.obj.typedobject import TypedObject

NETWISE_VERSION = 1
NETWISE_RELEASE = 0
NETWISE_INUMBER = 1094

version = "0.0.1 python"
handle = "localhost"


class DocbrokerClient(Netwise):
    def __init__(self, **kwargs):
        super(DocbrokerClient, self).__init__(**dict(kwargs, **{
            'version': NETWISE_VERSION,
            'release': NETWISE_RELEASE,
            'inumber': NETWISE_INUMBER,
        }))

    def get_docbase_map(self):
        return DocbaseMap(
            buffer=self._request_object(DocbrokerClient._docbase_map_request(version, handle))
        )

    def get_server_map(self, docbase):
        server_map = DocbaseMap(
            buffer=self._request_object(DocbrokerClient._server_map_request(version, handle, docbase)))
        if 'r_host_name' not in server_map:
            raise RuntimeError("No servers for docbase %s on %s" % (docbase, parse_address(server_map['i_host_addr'])))
        return server_map

    def _request_object(self, data):
        try:
            result = self.request(Request, type=1, data=[data]).next()
        finally:
            # Docbroker forcibly disconnects client after RPC
            self.disconnect()
        return result

    @staticmethod
    def _docbase_map_request(handle, version):
        obj = TypedObject(ser_version=0)
        obj.set_string("DBR_REQUEST_NAME", "DBRN_GET_DOCBASE_MAP")
        obj.set_int("DBR_REQUEST_VERSION", 1)
        obj.set_string("DBR_REQUEST_HANDLE", handle)
        obj.set_string("DBR_SOFTWARE_VERSION", version)
        return obj

    @staticmethod
    def _server_map_request(handle, version, docbase):
        obj = TypedObject(ser_version=0)
        obj.set_string("r_docbase_name", docbase)
        obj.set_string("r_map_name", "mn_cs_map")
        obj.set_string("DBR_REQUEST_NAME", "DBRN_GET_SERVER_MAP")
        obj.set_int("DBR_REQUEST_VERSION", 1)
        obj.set_string("DBR_REQUEST_HANDLE", handle)
        obj.set_string("DBR_SOFTWARE_VERSION", version)
        return obj
