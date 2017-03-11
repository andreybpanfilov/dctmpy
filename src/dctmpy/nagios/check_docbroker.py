#!/usr/bin/env python

import argparse
import re

from nagiosplugin import Check, Resource, guarded, Result, Metric, ScalarContext
from nagiosplugin.state import Critical, Ok, Unknown

from dctmpy import get_current_time_mills
from dctmpy.docbrokerclient import DocbrokerClient
from dctmpy.nagios import CheckSummary, NULL_CONTEXT, CIPHERS, TIME_THRESHOLDS


class CheckDocbroker(Resource):
    def __init__(self, args, results):
        self.args = args
        self.results = results

    def probe(self):
        yield Metric(NULL_CONTEXT, 0, context=NULL_CONTEXT)
        docbroker = DocbrokerClient(host=self.host, port=self.port, secure=self.args.secure, ciphers=CIPHERS)
        try:
            start = get_current_time_mills()
            docbasemap = docbroker.get_docbase_map()
            yield Metric('docbase_map_time', get_current_time_mills() - start, "ms", min=0, context=TIME_THRESHOLDS)

            if not docbasemap['r_docbase_name']:
                message = "No registered docbases"
                self.add_result(Unknown, message)
                return

            if not self.docbase:
                message = "Registered docbases: " + ",".join(docbasemap['r_docbase_name'])
                self.add_result(Ok, message)
                return

            servers = {}
            for definition in re.split(",\s*", self.docbase):
                docbase = None
                server = None
                address = None
                if '.' in definition:
                    (docbase, server) = definition.split('.', 1)
                    if '@' in server:
                        (server, address) = server.split('@')
                else:
                    docbase = definition
                if not servers.has_key(docbase):
                    servers[docbase] = []

                if (None, None) in servers[docbase]:
                    # we will check status of all servers
                    continue

                servers[docbase].append((server, address))

            for docbase in servers.keys():
                if docbase not in docbasemap['r_docbase_name']:
                    message = "Docbase %s is not registered on %s:%d" % \
                              (docbase, self.host, self.port)
                    self.add_result(Critical, message)
                    continue

                try:
                    servermap = docbroker.get_server_map(docbase)
                    for (server, address) in servers[docbase]:
                        if not server:
                            # server name was not defined - checking all servers
                            for i in xrange(0, len(servermap['r_server_name'])):
                                (name, host, status) = self.get_server_info(servermap, i)
                                if status == 'Open':
                                    message = "Server %s.%s is registered on %s:%d" % \
                                              (docbase, name, self.host, self.port)
                                    self.add_result(Ok, message)
                                    continue
                                message = "Server %s.%s has status %s on %s:%d" % \
                                          (docbase, name, status, self.host, self.port)
                                self.add_result(Critical, message)
                            break

                        if server not in servermap['r_server_name']:
                            message = "Server %s.%s is not registered on %s:%d" % \
                                      (docbase, server, self.host, self.port)
                            self.add_result(Critical, message)
                            continue

                        index = servermap['r_server_name'].index(server)
                        (_, host, status) = self.get_server_info(servermap, index)
                        if address and host != address:
                            message = "Server %s.%s (status: %s) is registered on %s:%d " \
                                      "with wrong ip address: %s, expected: %s" % \
                                      (docbase, server, status, self.host, self.port, host, address)
                            self.add_result(Critical, message)
                            continue
                        if status == 'Open':
                            message = "Server %s.%s@%s is registered on %s:%d" % \
                                      (docbase, server, host, self.host, self.port)
                            self.add_result(Ok, message)
                            continue
                        message = "Server %s.%s@%s has status %s on %s:%d" % \
                                  (docbase, server, host, status, self.host, self.port)
                        self.add_result(Critical, message)

                    if not self.fullmap:
                        continue

                    if (None, None) in servers[docbase]:
                        continue

                    # checking malicious servers
                    for i in xrange(0, len(servermap['r_server_name'])):
                        (server, host, status) = self.get_server_info(servermap, i)
                        if (server, host) not in servers[docbase] and (server, None) not in servers[docbase]:
                            message = "Malicious server %s.%s@%s (status: %s) is registered on %s:%d" % \
                                      (docbase, server, host, status, self.host, self.port)
                            self.add_result(Critical, message)

                except Exception, e:
                    message = "Failed to retrieve servermap for docbase %s: %s" % \
                              (docbase, str(e))
                    self.add_result(Critical, message)
                    continue

            # checking malicious docbases
            for docbase in docbasemap['r_docbase_name']:
                if docbase in servers:
                    continue
                message = "Malicious docbase %s is registered on %s:%d" % \
                          (docbase, self.host, self.port)
                self.add_result(Critical, message)

        except Exception, e:
            message = "Failed to retrieve docbasemap: %s" % str(e)
            self.add_result(Critical, message)

    def add_result(self, state, message):
        self.results.add(Result(state, message))

    def __getattr__(self, name):
        if hasattr(self.args, name):
            return getattr(self.args, name)
        else:
            return AttributeError("Unknown attribute %s in %s" % (name, str(self.__class__)))

    def get_server_info(self, servermap, index):
        return (servermap['r_server_name'][index], servermap['i_server_connection_address'][index].split(" ")[5],
                servermap['r_last_status'][index])


@guarded
def main():
    argp = argparse.ArgumentParser(description=__doc__)
    argp.add_argument('-H', '--host', required=True, metavar='hostname', help='server hostname')
    argp.add_argument('-p', '--port', required=False, metavar='port', default=1489, type=int,
                      help='server port, default 1489')
    argp.add_argument('-s', '--secure', action='store_true', help='use ssl')
    argp.add_argument('-f', '--fullmap', action='store_true',
                      help='check for malicious servers registered on docbroker, '
                           '-d/--docbase argument must specify all servers supposed to be registered')
    argp.add_argument('-d', '--docbase', required=False, metavar='docbase', help='docbase name')
    argp.add_argument('-n', '--name', metavar='name', default='', help='name of check that appears in output')
    argp.add_argument('-t', '--timeout', metavar='timeout', default=60, type=int,
                      help='check timeout, default is 60 seconds')
    argp.add_argument('-w', '--warning', metavar='RANGE', help='warning threshold')
    argp.add_argument('-c', '--critical', metavar='RANGE', help='critical threshold')
    args = argp.parse_args()

    if ':' in args.host:
        chunks = args.host.split(':')
        setattr(args, 'host', chunks[0])
        setattr(args, 'port', int(chunks[1]))

    check = Check(CheckSummary())
    if getattr(args, 'name', None):
        check.name = args.name
    check.add(CheckDocbroker(args, check.results))
    check.add(
        ScalarContext(
            TIME_THRESHOLDS,
            getattr(args, "warning"),
            getattr(args, "critical"),
            fmt_metric=fmt_metric
        )
    )
    check.main(timeout=args.timeout)


def fmt_metric(metric, context):
    if hasattr(metric, 'message'):
        return getattr(metric, 'message')
    return '{name} is {valueunit}'.format(
        name=metric.name, value=metric.value, uom=metric.uom,
        valueunit=metric.valueunit, min=metric.min, max=metric.max
    )


if __name__ == '__main__':
    main()
