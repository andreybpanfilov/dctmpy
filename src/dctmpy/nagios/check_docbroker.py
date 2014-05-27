#!/usr/bin/env python

import argparse
import re

from nagiosplugin import Check, Resource, guarded, Result, Metric
from nagiosplugin.state import Critical, Ok, Unknown

from dctmpy.docbrokerclient import DocbrokerClient
from dctmpy.nagios import CheckSummary


NULL_CONTEXT = 'null'


class CheckDocbroker(Resource):
    def __init__(self, args, results):
        self.args = args
        self.results = results

    def probe(self):
        yield Metric(NULL_CONTEXT, 0, context=NULL_CONTEXT)
        docbroker = DocbrokerClient(host=self.host, port=self.port)
        try:
            docbasemap = docbroker.get_docbasemap()

            if not docbasemap['r_docbase_name']:
                message = "No registered docbases"
                self.add_result(Unknown, message)
                return

            if not self.docbase:
                message = "Registered docbases: " + ",".join(docbasemap['r_docbase_name'])
                self.add_result(Ok, message)
                return

            for docbase in re.split(',\s*', self.docbase):
                server = None

                if '.' in docbase:
                    chunks = docbase.split('.')
                    docbase = chunks[0]
                    server = chunks[1]

                if docbase not in docbasemap['r_docbase_name']:
                    message = "Docbase %s is not registered on %s:%d" % (docbase, self.host, self.port)
                    self.add_result(Critical, message)
                    continue

                if not server:
                    message = "Docbase %s is registered on %s:%d" % (docbase, self.host, self.port)
                    self.add_result(Ok, message)
                    continue

                try:
                    servermap = docbroker.get_servermap(docbase)
                    if server not in servermap['r_server_name']:
                        message = "Server %s.%s is not registered on %s:%d" % (docbase, server, self.host, self.port)
                        self.add_result(Critical, message)
                    else:
                        index = servermap['r_server_name'].index(server)
                        status = servermap['r_last_status'][index]
                        if status == 'Open':
                            message = "Server %s.%s is registered on %s:%d" % (
                                docbase, server, self.host, self.port)
                            self.add_result(Ok, message)
                        else:
                            message = "Server %s.%s has status %s on %s:%d" % (
                                docbase, server, status, self.host, self.port)
                            self.add_result(Ok, message)
                    continue
                except Exception, e:
                    message = "Failed to retrieve servermap for docbase %s: %s" % (docbase, str(e))
                    self.add_result(Critical, message)
                    continue

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


@guarded
def main():
    argp = argparse.ArgumentParser(description=__doc__)
    argp.add_argument('-H', '--host', required=True, metavar='hostname', help='server hostname')
    argp.add_argument('-p', '--port', required=False, metavar='port', default=1489, type=int,
                      help='server port, default 1489')
    argp.add_argument('-d', '--docbase', required=False, metavar='docbase', help='docbase name')
    argp.add_argument('-n', '--name', metavar='name', default='', help='name of check that appears in output')
    argp.add_argument('-t', '--timeout', metavar='timeout', default=60, type=int,
                      help='check timeout, default is 60 seconds')
    args = argp.parse_args()

    if ':' in args.host:
        chunks = args.host.split(':')
        setattr(args, 'host', chunks[0])
        setattr(args, 'port', int(chunks[1]))

    check = Check(CheckSummary())
    if getattr(args, 'name', None):
        check.name = args.name
    check.add(CheckDocbroker(args, check.results))
    check.main(timeout=args.timeout)


if __name__ == '__main__':
    main()