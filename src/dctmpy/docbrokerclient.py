#  Copyright (c) 2013 Andrey B. Panfilov <andrew@panfilov.tel>
#
#  See main module for license.
#
from dctmpy import parseAddr, AttrValue, STRING, INT
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

    def getDocbaseMap(self):
        return DocbaseMap(
            buffer=self.requestObject(DocbrokerClient.docbaseMapRequest(version, handle))
        )

    def getServerMap(self, docbase):
        servermap = DocbaseMap(buffer=self.requestObject(DocbrokerClient.serverMapRequest(version, handle, docbase)))
        if not 'r_host_name' in servermap:
            raise RuntimeError("No servers for docbase %s on %s" % (docbase, parseAddr(servermap['i_host_addr'])))
        return servermap

    def requestObject(self, data):
        try:
            result = self.request(type=1, data=[data], immediate=True).receive().next()
        finally:
            self.disconnect()
        return result

    @staticmethod
    def docbaseMapRequest(handle, version):
        obj = TypedObject(serializationversion=0)
        obj.add(AttrValue(name="DBR_REQUEST_NAME", type=STRING, values=["DBRN_GET_DOCBASE_MAP"]))
        obj.add(AttrValue(name="DBR_REQUEST_VERSION", type=INT, values=[1]))
        obj.add(AttrValue(name="DBR_REQUEST_HANDLE", type=STRING, values=[handle]))
        obj.add(AttrValue(name="DBR_SOFTWARE_VERSION", type=STRING, values=[version]))
        return obj

    @staticmethod
    def serverMapRequest(handle, version, docbase):
        obj = TypedObject(serializationversion=0)
        obj.add(AttrValue(name="r_docbase_name", type=STRING, values=[docbase]))
        obj.add(AttrValue(name="r_map_name", type=STRING, values=["mn_cs_map"]))
        obj.add(AttrValue(name="DBR_REQUEST_NAME", type=STRING, values=["DBRN_GET_SERVER_MAP"]))
        obj.add(AttrValue(name="DBR_REQUEST_VERSION", type=INT, values=[1]))
        obj.add(AttrValue(name="DBR_REQUEST_HANDLE", type=STRING, values=[handle]))
        obj.add(AttrValue(name="DBR_SOFTWARE_VERSION", type=STRING, values=[version]))
        return obj


