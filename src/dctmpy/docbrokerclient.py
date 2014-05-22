#  Copyright (c) 2013 Andrey B. Panfilov <andrew@panfilov.tel>
#
#  See main module for license.
#
from dctmpy import parse_address, AttrValue, STRING, INT
from dctmpy.net.request import Request
from dctmpy.netwise import Netwise
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

    def get_docbasemap(self):
        return DocbaseMap(
            buffer=self._request_object(DocbrokerClient._docbasemap_request(version, handle))
        )

    def get_servermap(self, docbase):
        servermap = DocbaseMap(
            buffer=self._request_object(DocbrokerClient._servermap_request(version, handle, docbase)))
        if not 'r_host_name' in servermap:
            raise RuntimeError("No servers for docbase %s on %s" % (docbase, parse_address(servermap['i_host_addr'])))
        return servermap

    def _request_object(self, data):
        try:
            result = self.request(Request, type=1, data=[data], immediate=True).receive().next()
        finally:
            self.disconnect()
        return result

    @staticmethod
    def _docbasemap_request(handle, version):
        obj = TypedObject(serversion=0)
        obj.add(AttrValue(name="DBR_REQUEST_NAME", type=STRING, values=["DBRN_GET_DOCBASE_MAP"]))
        obj.add(AttrValue(name="DBR_REQUEST_VERSION", type=INT, values=[1]))
        obj.add(AttrValue(name="DBR_REQUEST_HANDLE", type=STRING, values=[handle]))
        obj.add(AttrValue(name="DBR_SOFTWARE_VERSION", type=STRING, values=[version]))
        return obj

    @staticmethod
    def _servermap_request(handle, version, docbase):
        obj = TypedObject(serversion=0)
        obj.add(AttrValue(name="r_docbase_name", type=STRING, values=[docbase]))
        obj.add(AttrValue(name="r_map_name", type=STRING, values=["mn_cs_map"]))
        obj.add(AttrValue(name="DBR_REQUEST_NAME", type=STRING, values=["DBRN_GET_SERVER_MAP"]))
        obj.add(AttrValue(name="DBR_REQUEST_VERSION", type=INT, values=[1]))
        obj.add(AttrValue(name="DBR_REQUEST_HANDLE", type=STRING, values=[handle]))
        obj.add(AttrValue(name="DBR_SOFTWARE_VERSION", type=STRING, values=[version]))
        return obj


