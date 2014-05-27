#!/usr/bin/env python

import argparse
import re
import time

try:
    from urllib.request import urlopen, URLError
except ImportError:
    from urllib2 import urlopen, URLError

try:
    from urllib import urlencode
except ImportError:
    urlencode = None

from xml.dom import minidom

from nagiosplugin import Metric, Result, Summary, Check, Resource, guarded, ScalarContext
from nagiosplugin.state import Critical, Warn, Ok, Unknown

from dctmpy.docbaseclient import DocbaseClient
from dctmpy.docbrokerclient import DocbrokerClient


THRESHOLDS = 'thresholds'
NULL_CONTEXT = 'null'

JOB_ATTRIBUTES = ['object_name', 'is_inactive', 'a_last_invocation',
                  'a_last_completion', 'a_last_return_code', 'a_current_status',
                  'a_status', 'a_special_app', 'run_mode', 'run_interval',
                  'expiration_date', 'max_iterations', 'a_iterations',
                  'a_next_invocation', 'start_date', 'a_current_status']

JOB_QUERY = "SELECT " + ", ".join(JOB_ATTRIBUTES) + " FROM dm_job"

JOB_ACTIVE_CONDITION = "((a_last_invocation IS NOT NULLDATE and a_last_completion IS NULLDATE) " \
                       " OR a_special_app = 'agentexec')" \
                       " AND (i_is_reference = 0 OR i_is_reference is NULL)" \
                       " AND (i_is_replica = 0 OR i_is_replica is NULL)"

JOB_INTERVALS = {
    1: 60,
    2: 60 * 60,
    3: 24 * 60 * 60,
    4: 7 * 24 * 60 * 60
}

CTS_ATTRIBUTES = ['r_object_id', 'object_name', 'cts_version', 'agent_url',
                  'hostname', 'status', 'websrv_url', 'inst_type']

CTS_QUERY = "SELECT " + ", ".join(CTS_ATTRIBUTES) + " FROM cts_instance_info"

APP_SERVER_RESPONSES = {
    'do_method': 'Documentum Java Method Server',
    'do_mail': 'Documentum Mail Servlet',
    'do_bpm': 'Documentum Java Method Server',
    'acs': 'ACS Server Is Running',
    'dsearch': 'The xPlore instance',
}

APP_SERVER_TIMEOUT = 5


class CheckDocbase(Resource):
    def __init__(self, args, results):
        self.args = args
        self.results = results
        self.session = None

    def probe(self):
        yield Metric(NULL_CONTEXT, 0, context=NULL_CONTEXT)
        try:
            self.check_login()
            if not self.session:
                return
            if self.mode == 'login':
                return
            results = modes[self.mode][0](self)
            if not results:
                return
            for result in results:
                if result:
                    yield result
        finally:
            try:
                if self.session:
                    self.session.disconnect()
            except Exception, e:
                pass

    def check_sessions(self):
        try:
            count = self.session.count_sessions()
        except Exception, e:
            self.add_result(Critical, "Unable to retrieve session count: " + str(e))
            return
        yield Metric('sessioncount', int(count['hot_list_size']), min=0,
                     max=int(count['concurrent_sessions']),
                     context=THRESHOLDS)

    def check_targets(self):
        targets = []
        server_name = self.session.serverconfig['object_name']
        docbase_name = self.session.docbaseconfig['object_name']
        try:
            for target in self.session.list_targets():
                targets.extend(zip(target['projection_targets'], target['projection_ports']))
        except Exception, e:
            message = "Unable to retrieve targets: %s" % str(e)
            self.add_result(Critical, message)
            return

        for (host, port) in targets:
            self.check_registration(host, port, docbase_name, server_name)

    def check_registration(self, docbrokerhost, docbrokerport, docbaseame, servername):
        docbroker = DocbrokerClient(host=docbrokerhost, port=docbrokerport)

        try:
            docbasemap = docbroker.get_docbasemap()
        except Exception, e:
            message = "Unable to retrieve docbasemap from docbroker %s:%d: %s" % (
                docbrokerhost, docbrokerport, str(e))
            self.add_result(Critical, message)
            return

        if not docbaseame in docbasemap['r_docbase_name']:
            message = "docbase %s is not registered on %s:%d" % (docbaseame, docbrokerhost, docbrokerport)
            self.add_result(Critical, message)
            return

        try:
            servermap = docbroker.get_servermap(docbaseame)
        except Exception, e:
            message = "Unable to retrieve servermap from docbroker %s:%d: %s" % (
                docbrokerhost, docbrokerport, str(e))
            self.add_result(Critical, message)
            return

        if not servername in servermap['r_server_name']:
            message = "server %s.%s is not registered on %s:%d" % (
                docbaseame, servername, docbrokerhost, docbrokerport)
            self.add_result(Critical, message)
            return

        index = servermap['r_server_name'].index(servername)
        status = servermap['r_last_status'][index]
        docbaseid = servermap['i_docbase_id'][index]
        connaddr = servermap['i_server_connection_address'][index]

        if status != "Open":
            message = "%s.%s has status %s on %s:%d, " % (
                docbaseame, servername, status, docbrokerhost, docbrokerport)
            self.add_result(Critical, message)
            return

        chunks = connaddr.split(" ")
        host = chunks[5]
        port = int(chunks[2], 16)

        session = None
        try:
            session = DocbaseClient(host=host, port=port, docbaseid=docbaseid)
            message = "%s.%s has status %s on %s:%d" % (
                docbaseame, servername, status, docbrokerhost, docbrokerport)
            self.add_result(Ok, message)
        except Exception, e:
            message = "%s.%s has status %s on %s:%d, but error occurred during connection to %s" % (
                docbaseame, servername, status, docbrokerhost, docbrokerport, str(e))
            self.add_result(Critical, message)
            return
        if session:
            try:
                session.disconnect()
            except Exception, e:
                pass

    def check_index_agents(self):
        try:
            count = 0
            for index in CheckDocbase.get_indexes(self.session):
                count += 1
                self.check_index_agent(index['index_name'], index['object_name'])
            if count == 0:
                message = "No indexagents"
                self.add_result(Warn, message)
        except Exception, e:
            message = "Unable to execute query: %s" % str(e)
            self.add_result(Critical, message)

    def check_index_agent(self, index_name, agent_name):
        try:
            result = self.session.ftindex_agent_admin(
                index_name, agent_name
            )
            status = result['status'][0]
            if status == 0:
                message = "Indexagent %s/%s is up and running" % (index_name, agent_name)
                self.add_result(Ok, message)
            elif status == 100:
                message = "Indexagent %s/%s is stopped" % (index_name, agent_name)
                self.add_result(Warn, message)
            elif status == 200:
                message = "A problem with indexagent %s/%s" % (index_name, agent_name)
                self.add_result(Critical, message)
            else:
                message = "Indexagent %s/%s has unknown status" % (index_name, agent_name)
                self.add_result(Unknown, message)
        except Exception, e:
            message = "Unable to get indexagent %s/%s status: %s" % (
                index_name, agent_name, str(e))
            self.add_result(Critical, message)

    def check_jobs(self):
        jobs_to_check = None
        if not CheckDocbase.is_empty(self.jobs):
            if isinstance(self.jobs, list):
                jobs_to_check = list(self.jobs)
            elif isinstance(self.jobs, str):
                jobs_to_check = re.split(',\s*', self.jobs)
            else:
                raise RuntimeError("Wrong jobs argument")

        try:
            now = self.session.time()
        except Exception, e:
            message = "Unable to acquire current time: %s" % str(e)
            self.add_result(Critical, message)
            return

        try:
            for job in CheckDocbase.get_jobs(self.session, jobs_to_check):
                if jobs_to_check and job['object_name'] in jobs_to_check:
                    jobs_to_check.remove(job['object_name'])
                self.check_job(job, now)
        except Exception, e:
            message = "Unable to execute query: %s" % str(e)
            self.add_result(Critical, message)
            return

        if not CheckDocbase.is_empty(jobs_to_check):
            message = ""
            for job in jobs_to_check:
                message += "%s not found, " % job
            self.add_result(Critical, message)

    def check_job(self, job, now):
        if not job['start_date'] > -1:
            message = "%s has undefined start_date" % job['object_name']
            self.add_result(Critical, message)
            return
        if not job['a_next_invocation'] > -1:
            message = "%s has undefined next_invocation_date" % job['object_name']
            self.add_result(Critical, message)
            return
        if -1 < job['expiration_date'] < job['start_date']:
            message = "%s has expiration_date less then start_date" % job['object_name']
            self.add_result(Critical, message)
            return
        if job['max_iterations'] < 0:
            message = "%s has invalid max_iterations value: %d" % (
                (job['object_name']), (job['max_iterations']))
            self.add_result(Critical, message)
            return
        if job['run_mode'] == 0 and job['run_interval'] == 0 and job['max_iterations'] != 1:
            message = "%s has invalid max_iterations value for run_mode=0 and run_interval=0" % job[
                'object_name']
            self.add_result(Critical, message)
            return
        if job['run_mode'] in [1, 2, 3, 4] and not (1 <= job['run_interval'] <= 32767):
            message = "%s has invalid run_interval value, expected [1, 32767], got %d" % (
                (job['object_name']), (job['run_interval']))
            self.add_result(Critical, message)
            return
        if job['run_mode'] == 7 and not (-7 <= job['run_interval'] <= 7 and job['run_interval'] != 0):
            message = "%s has invalid run_interval value, expected [-7,0) U (0,7], got %d" % (
                (job['object_name']), (job['run_interval']))
            self.add_result(Critical, message)
            return
        if job['run_mode'] == 8 and not (-28 <= job['run_interval'] <= 28 and job['run_interval'] != 0):
            message = "%s has invalid run_interval value, expected [-28,0) U (0,28], got %d" % (
                (job['object_name']), (job['run_interval']))
            self.add_result(Critical, message)
            return
        if job['run_mode'] == 9 and not (-365 <= job['run_interval'] <= 365 and job['run_interval'] != 0):
            message = "%s has invalid run_interval value, expected [-365,0) U (0,365], got %d" % (
                (job['object_name']), (job['run_interval']))
            self.add_result(Critical, message)
            return
        if job['is_inactive']:
            message = "%s is inactive" % job['object_name']
            self.add_result(Critical, message)
            return
        if now > job['expiration_date'] > -1:
            message = "%s is expired" % job['object_name']
            self.add_result(Critical, message)
            return
        if 0 < job['max_iterations'] < job['a_iterations']:
            message = "%s max iterations exceeded" % job['object_name']
            self.add_result(Critical, message)
            return
        if not job['a_last_invocation'] > -1:
            message = "%s has been never executed" % job['object_name']
            self.add_result(Warn, message)
            return
        if job['a_last_return_code'] != 0:
            message = "%s has status: %s" % ((job['object_name']), (job['a_current_status']))
            self.add_result(Critical, message)
            return
        if re.match('agentexec', job['a_special_app']) or (
                    job['a_last_invocation'] > -1 and not job['a_last_completion']):
            message = "%s is running for %s" % (
                (job['object_name']), CheckDocbase.pretty_interval(now - job['a_last_invocation']))
            self.add_result(Ok, message)
            return

        time_diff = now - job['a_last_completion']

        if job['run_mode'] in [1, 2, 3, 4]:
            message = "%s last run - %s ago" % ((job['object_name']), CheckDocbase.pretty_interval(time_diff))
            if time_diff > 2 * JOB_INTERVALS[job['run_mode']] * job['run_interval']:
                self.add_result(Critical, message)
                return
            elif time_diff > JOB_INTERVALS[job['run_mode']] * job['run_interval']:
                self.add_result(Warn, message)
                return
            else:
                self.add_result(Ok, message)
                return
        else:
            message = "Scheduling type for job %s is not currently supported" % job['object_name']
            self.add_result(Critical, message)
            return

    def check_time_skew(self):
        try:
            server_time = self.session.time()
            yield Metric('timeskew', abs(server_time - time.time()), min=0, context=THRESHOLDS)
        except Exception, e:
            message = "Unable to acquire current time: %s" % str(e)
            self.add_result(Critical, message)
            return

    def check_method(self):
        if not re.match("execute do_method", self.query, re.IGNORECASE):
            message = "Wrong query: %s" % self.query
            self.add_result(Critical, message)
            return
        try:
            result = CheckDocbase.read_object(self.session, self.query)
            message = self.format.format(**result)
            if result['launch_failed']:
                self.add_result(Critical, message)
            else:
                self.add_result(Ok, message)
        except Exception, e:
            message = "Unable to execute query: %s" % str(e)
            self.add_result(Critical, message)

    def check_query(self):
        try:
            count = 0
            message = ""
            for rec in CheckDocbase.read_query(self.session, self.query):
                count += 1
                if count > 1:
                    message += ", "
                message += self.format.format(**rec)
            yield CustomMetric('count', count, min=0, context=THRESHOLDS).add_message(message)
        except Exception, e:
            message = "Unable to execute query: %s" % str(e)
            self.add_result(Critical, message)

    def check_count_query(self):
        try:
            result = CheckDocbase.read_object(self.session, self.query)
            yield Metric('countquery', int(result.values().pop()), min=0, context=THRESHOLDS)
        except Exception, e:
            message = "Unable to execute query: %s" % str(e)
            self.add_result(Critical, message)

    def check_work_queue(self):
        query = "SELECT count(r_object_id) AS work_queue_size FROM dmi_workitem " \
                "WHERE r_runtime_state IN (0, 1) " \
                "AND r_auto_method_id > '0000000000000000' " \
                "AND a_wq_name is NULLSTRING"
        try:
            result = CheckDocbase.read_object(self.session, query)
            yield Metric('workqueue', int(result['work_queue_size']), min=0, context=THRESHOLDS)
        except Exception, e:
            message = "Unable to execute query: %s" % str(e)
            self.add_result(Critical, message)

    def check_server_work_queue(self):
        server_id = self.session.serverconfig['r_object_id']
        server_name = self.session.serverconfig['object_name']
        query = "SELECT count(r_object_id) AS work_queue_size FROM dmi_workitem " \
                "WHERE r_runtime_state IN (0, 1) " \
                "AND r_auto_method_id > '0000000000000000' " \
                "AND a_wq_name ='" + server_id + "'"
        try:
            result = CheckDocbase.read_object(self.session, query)
            yield Metric(server_name[-20:], int(result['work_queue_size']), min=0, context=THRESHOLDS)
        except Exception, e:
            message = "Unable to execute query: %s" % str(e)
            self.add_result(Critical, message)

    def check_fulltext_queue(self):
        query = "select distinct queue_user from dm_ftindex_agent_config"
        try:
            count = 0
            for user in CheckDocbase.read_query(self.session, query):
                count += 1
                result = self.check_fulltext_queue_for_user(user['queue_user'])
                if result:
                    yield result
            if count == 0:
                message = "No indexagents"
                self.add_result(Warn, message)
        except Exception, e:
            message = "Unable to execute query: %s" % str(e)
            self.add_result(Critical, message)

    def check_fulltext_queue_for_user(self, username):
        query = "SELECT count(r_object_id) AS queue_size FROM dmi_queue_item WHERE name='" \
                + username + "'AND task_state not in ('failed','warning')"
        try:
            result = CheckDocbase.read_object(self.session, query)
            return Metric(username, int(result['queue_size']), min=0, context=THRESHOLDS)
        except Exception, e:
            message = "Unable to execute query: %s" % str(e)
            self.add_result(Critical, message)

    def check_cts_queue(self):
        query = "SELECT count(r_object_id) AS queue_size " \
                "FROM dmi_queue_item WHERE name='dm_mediaserver' AND delete_flag=FALSE"
        try:
            result = CheckDocbase.read_object(self.session, query)
            yield Metric("dm_mediaserver", int(result['queue_size']), min=0, context=THRESHOLDS)
        except Exception, e:
            message = "Unable to execute query: %s" % str(e)
            self.add_result(Critical, message)

    def check_failed_tasks(self):
        try:
            count = 0
            message = ""
            for rec in CheckDocbase.get_failed_tasks(self.session):
                count += 1
                if count > 1:
                    message += ", "
                message += "'%s' (%s)" % (rec['task_name'], rec['name'])

            if count > 0:
                message = "%d task(s): %s" % (count, message)
                self.add_result(Critical, message)
        except Exception, e:
            message = "Unable to execute query: %s" % str(e)
            self.add_result(Critical, message)

    def check_jms_status(self):
        if not 'DUMP_JMS_CONFIG_LIST' in self.session.entrypoints:
            self.check_jms_status_old()
        else:
            self.check_jms_status_new()

    def check_jms_status_new(self):
        try:
            config = self.session.dump_jms_config_list()
            for i in xrange(0, len(config['jms_config_id'])):
                if config['is_disabled_in_docbase'][i]:
                    message = "%s is disabled" % config['jms_config_name'][i]
                    self.add_result(Warn, message)
                    continue
                if config['is_marked_dead_in_cache'][i]:
                    message = "%s is marked as dead" % config['jms_config_name'][i]
                    self.add_result(Critical, message)
                else:
                    try:
                        jmsconfig = self.session.fetch(config['jms_config_id'][i])
                        for i in xrange(0, len(jmsconfig['servlet_name'])):
                            name = jmsconfig['servlet_name'][i]
                            url = jmsconfig['base_uri'][i]
                            self.check_app_server(name, url)
                    except Exception, e:
                        message = "Unable to retrieve jms config %s: %s" % (config['jms_config_id'][i], str(e))
                        self.add_result(Critical, message)

        except Exception, e:
            message = "Unable to retrieve jms configs: %s" % str(e)
            self.add_result(Critical, message)
            return

    def check_jms_status_old(self):
        serverconfig = self.session.serverconfig
        for i in xrange(0, len(serverconfig['app_server_name'])):
            name = serverconfig['app_server_name'][i]
            url = serverconfig['app_server_uri'][i]
            self.check_app_server(name, url)

    def check_acs_status(self):
        try:
            count = 0
            for rec in CheckDocbase.get_acs_configs(self.session):
                count += 1
                try:
                    acs = self.session.fetch(rec['r_object_id'])
                    for url in acs['acs_base_url']:
                        self.check_app_server('acs', url)
                except Exception, e:
                    message = "Unable to retrieve acs config %s: %s" % (rec['r_object_id'], str(e))
                    self.add_result(Critical, message)
            if count == 0:
                message = "No ACS instances"
                self.add_result(Warn, message)
        except Exception, e:
            message = "Unable to execute query: %s" % str(e)
            self.add_result(Critical, message)
            return

    def check_xplore_status(self):
        try:
            count = 0
            for rec in CheckDocbase.get_xplore_configs(self.session):
                count += 1
                try:
                    xplore = self.session.fetch(rec['r_object_id'])
                    prop = dict(zip(xplore['param_name'], xplore['param_value']))
                    url = "%s://%s:%s%s" % (
                        prop['dsearch_qrserver_protocol'].lower(),
                        prop['dsearch_qrserver_host'],
                        prop['dsearch_qrserver_port'],
                        prop['dsearch_qrserver_target']
                    )
                    self.check_app_server('dsearch', url)
                except Exception, e:
                    message = "Unable to retrieve ft_engine config %s: %s" % (rec['r_object_id'], str(e))
                    self.add_result(Critical, message)
            if count == 0:
                message = "No xPlore instances"
                self.add_result(Warn, message)
        except Exception, e:
            message = "Unable to execute query: %s" % str(e)
            self.add_result(Critical, message)
            return

    def check_cts_status(self):
        try:
            count = 0
            for cts in CheckDocbase.get_cts_instances(self.session):
                count += 1
                self.check_cts(cts)
            if count == 0:
                message = "No CTS instances"
                self.add_result(Warn, message)
        except Exception, e:
            message = "Unable to execute query: %s" % str(e)
            self.add_result(Critical, message)
            return

    def check_cts(self, cts):
        try:
            ticket = self.session.get_login()
        except Exception, e:
            message = "Unable to create login ticket: %s" % str(e)
            self.add_result(Critical, message)
            return

        data = {
            'docbase': self.session.docbaseconfig['object_name'],
            'userid': self.session.username,
            'ticket': ticket,
            'instanceid': cts['r_object_id'],
            'command': 'GET_STATUS',
        }

        url = cts['agent_url']
        instance_name = cts['object_name']
        try:
            if urlencode:
                response = urlopen(url, data=urlencode(data), timeout=APP_SERVER_TIMEOUT)
            else:
                response = urlopen(url, data=data, timeout=APP_SERVER_TIMEOUT)
            if response.code != 200:
                message = "Unable to open %s (instance: %s): response code %d" % (url, instance_name, response.code)
                self.add_result(Critical, message)
                return

            try:
                xmldoc = minidom.parseString(response.read())
                status = xmldoc.getElementsByTagName("message-id")[0].firstChild.nodeValue
                message = "CTS Agent for instance \"%s\" has status \"%s\"" % (instance_name, status)
                if status != "RUNNING":
                    self.add_result(Critical, message)
                else:
                    self.add_result(Ok, message)
            except Exception, e:
                message = "Unable to parse xml response (instance: %s): %s" % (instance_name, str(e))
                self.add_result(Critical, message)

        except URLError, e:
            message = "Unable to open %s (instance: %s): %s" % (url, instance_name, e.reason)
            self.add_result(Critical, message)
        except Exception, e:
            message = "Unable to open %s (instance: %s): %s" % (url, instance_name, str(e))
            self.add_result(Critical, message)

    def check_app_server(self, name, url):
        if name not in APP_SERVER_RESPONSES:
            return
        try:
            serverconfig = self.session.serverconfig
            url = re.sub(r'(https?://)(localhost(\.localdomain)?|127\.0\.0\.1)([:/])?',
                         r'\1' + serverconfig['r_host_name'] + r'\4', url)
            response = urlopen(url, timeout=APP_SERVER_TIMEOUT)
            if not (response.code >= 200 and response < 300):
                message = "Unable to open %s: response code %d" % (url, response.code)
                self.add_result(Critical, message)
                return

            expected = APP_SERVER_RESPONSES[name]
            if expected not in response.read():
                message = "text \"%s\" not found in response from %s" % (expected, url)
                self.add_result(Critical, message)
                return

            message = "%s - OK" % url
            self.add_result(Ok, message)

        except URLError, e:
            message = "Unable to open %s: %s" % (url, e.reason)
            self.add_result(Critical, message)
        except Exception, e:
            message = "Unable to open %s: %s" % (url, str(e))
            self.add_result(Critical, message)

    def check_login(self):
        try:
            session = DocbaseClient(host=self.host, port=self.port, docbaseid=self.docbaseid)
        except Exception, e:
            message = "Unable to connect to docbase: %s" % str(e)
            self.add_result(Critical, message)
            return

        if self.login and self.authentication:
            try:
                session.authenticate(self.login, self.authentication)
            except Exception, e:
                message = "Unable to authenticate: %s" % str(e)
                self.add_result(Critical, message)
                try:
                    session.disconnect()
                except Exception, e:
                    pass
                return
            self.session = session
        else:
            message = ["No username provided", "No password provided"][not self.authentication]
            status = [Warn, Critical][self.mode != 'login']
            self.add_result(status, message)

    def add_result(self, state, message):
        self.results.add(Result(state, message))

    def __getattr__(self, name):
        if hasattr(self.args, name):
            return getattr(self.args, name)
        else:
            return AttributeError("Unknown attribute %s in %s" % (name, str(self.__class__)))

    @staticmethod
    def get_acs_configs(session):
        serverconfig = session.serverconfig
        query = "SELECT r_object_id FROM dm_acs_config" \
                " WHERE svr_config_id='%s'" % serverconfig['r_object_id']
        return CheckDocbase.read_query(session, query)

    @staticmethod
    def get_xplore_configs(session):
        query = "SELECT ft_engine_id as r_object_id FROM dm_fulltext_index WHERE install_loc='dsearch'"
        return CheckDocbase.read_query(session, query)

    @staticmethod
    def get_indexes(session):
        query = "select index_name, a.object_name " \
                "from dm_fulltext_index i, dm_ftindex_agent_config a " \
                "where i.index_name=a.index_name " \
                "and a.force_inactive = false"
        return CheckDocbase.read_query(session, query)

    @staticmethod
    def read_query(session, query, cnt=0):
        col = None
        try:
            read = 0
            col = session.query(query)
            for rec in col:
                yield (lambda x: dict((attr, x[attr]) for attr in x))(rec)
                read += 1
                if 0 < cnt == read:
                    break
        finally:
            if col:
                col.close()

    @staticmethod
    def read_object(session, query):
        results = CheckDocbase.read_query(session, query, 1)
        if results:
            for rec in results:
                return rec

    @staticmethod
    def get_jobs(session, jobs=None, condition=""):
        query = JOB_QUERY
        if CheckDocbase.is_empty(condition):
            if jobs:
                query += " WHERE object_name IN ('" + "','".join(jobs) + "')"
        else:
            query += " WHERE (%s)" % condition
            if jobs:
                query += " AND object_name IN ('" + "','".join(jobs) + "')"
        return CheckDocbase.read_query(session, query)

    @staticmethod
    def get_cts_instances(session, cts_names=None, condition=""):
        query = CTS_QUERY
        if CheckDocbase.is_empty(condition):
            if cts_names:
                query += " WHERE object_name IN ('" + "','".join(cts_names) + "')"
        else:
            query += " WHERE (%s)" % condition
            if cts_names:
                query += " AND object_name IN ('" + "','".join(cts_names) + "')"
        return CheckDocbase.read_query(session, query)

    @staticmethod
    def get_running_jobs(session):
        return CheckDocbase.get_jobs(session, JOB_ACTIVE_CONDITION)

    @staticmethod
    def get_failed_tasks(session, offset=None):
        query = "SELECT que.task_name, que.name" \
                " FROM dmi_queue_item que, dmi_workitem wi, dmi_package pkg" \
                " WHERE que.event = 'dm_changedactivityinstancestate'" \
                " AND que.item_id LIKE '4a%%'" \
                " AND que.message LIKE 'Activity instance, %%, of workflow, %%, failed.'" \
                " AND que.item_id = wi.r_object_id" \
                " AND wi.r_workflow_id = pkg.r_workflow_id" \
                " AND wi.r_act_seqno = pkg.r_act_seqno" \
                " AND que.delete_flag = 0"
        if offset >= 0:
            query += " que.date_sent > date(now) - %d " % offset
        return CheckDocbase.read_query(session, query)


    @staticmethod
    def pretty_interval(delta):
        if delta >= 0:
            secs = (delta) % 60
            mins = (int((delta) / 60)) % 60
            hours = (int((delta) / 3600))
            if hours < 24:
                return "%02d:%02d:%02d" % (hours, mins, secs)
            else:
                days = int(hours / 24)
                hours -= days * 24
                return "%d days %02d:%02d:%02d" % (days, hours, mins, secs)
        return "future"

    @staticmethod
    def is_empty(value):
        if value is None:
            return True
        if isinstance(value, str):
            if len(value) == 0:
                return True
            elif value.isspace():
                return True
            else:
                return False
        if isinstance(value, list):
            if len(value) == 0:
                return True
            else:
                return False
        if isinstance(value, dict):
            if len(value) == 0:
                return True
            else:
                return False
        return False


class CheckSummary(Summary):
    def verbose(self, results):
        return ''

    def ok(self, results):
        return self.format(results)

    def problem(self, results):
        return self.format(results)

    def format(self, results):
        message = ""
        for state in [Ok, Unknown, Warn, Critical]:
            hint = ", ".join(str(x) for x in results if x.state == state and not CheckDocbase.is_empty(str(x)))
            message = ", ".join(x for x in [hint, message] if not (CheckDocbase.is_empty(x)))
        return message


modes = {
    'sessioncount': [CheckDocbase.check_sessions, True, "check active session count"],
    'targets': [CheckDocbase.check_targets, False, "check whether server is registered on projection targets"],
    'indexagents': [CheckDocbase.check_index_agents, False, "check index agent status"],
    'jobs': [CheckDocbase.check_jobs, False, "check job scheduling"],
    'timeskew': [CheckDocbase.check_time_skew, True, "check time skew between nagios host and documentum"],
    'query': [CheckDocbase.check_query, True, "check results returned by query"],
    'method': [CheckDocbase.check_method, True, "check execution of method"],
    'countquery': [CheckDocbase.check_count_query, True, "check results returned by query"],
    'workqueue': [CheckDocbase.check_work_queue, True, "check workqueue size"],
    'serverworkqueue': [CheckDocbase.check_server_work_queue, True, "check server workqueue size"],
    'indexqueue': [CheckDocbase.check_fulltext_queue, True, "check index agent queue size"],
    'ctsqueue': [CheckDocbase.check_cts_queue, True, "check CTS agent queue size"],
    'failedtasks': [CheckDocbase.check_failed_tasks, True, "check failed tasks"],
    'login': [CheckDocbase.check_login, False, "check login"],
    'jmsstatus': [CheckDocbase.check_jms_status, False, "check JMS connectivity"],
    'ctsstatus': [CheckDocbase.check_cts_status, False, "check CTS connectivity"],
    'acsstatus': [CheckDocbase.check_acs_status, False, "check ACS connectivity"],
    'xplorestatus': [CheckDocbase.check_xplore_status, False, "check xPlore connectivity"],
}


@guarded
def main():
    argp = argparse.ArgumentParser(description=__doc__)
    argp.add_argument('-H', '--host', required=True, metavar='hostname', help='server hostname')
    argp.add_argument('-p', '--port', required=False, metavar='port', type=int, help='server port')
    argp.add_argument('-i', '--docbaseid', required=False, metavar='docbaseid', type=int, help='docbase identifier')
    argp.add_argument('-l', '--login', metavar='username', help='username')
    argp.add_argument('-a', '--authentication', metavar='password', help='password')
    #todo add ssl support
    #argp.add_argument('-s', '--secure', action='store_true', help='use ssl')
    argp.add_argument('-t', '--timeout', metavar='timeout', default=60, type=int,
                      help='check timeout, default is 60 seconds')
    argp.add_argument('-m', '--mode', required=True, metavar='mode',
                      help="mode to use, one of: " + ", ".join(x for x in modes.keys()))
    argp.add_argument('-j', '--jobs', metavar='jobs', default='', help='jobs to check, comma-separated list')
    argp.add_argument('-n', '--name', metavar='name', default='', help='name of check that appears in output')
    argp.add_argument('-q', '--query', metavar='query', default='', help='query to run')
    argp.add_argument('-f', '--format', metavar='format', default='', help='query output format')
    argp.add_argument('-w', '--warning', metavar='RANGE', help='warning threshold')
    argp.add_argument('-c', '--critical', metavar='RANGE', help='critical threshold')
    args = argp.parse_args()

    m = re.match('^(dctm(s)?://((.*?)(:(.*))?@)?)?([^/:]+?)(:(\d+))?(/(\d+))?$', args.host)
    if m:
        if m.group(2):
            #setattr(args, 'secure', m.group(2))
            pass
        if m.group(4):
            setattr(args, 'login', m.group(4))
        if m.group(6):
            setattr(args, 'authentication', m.group(6))
        if m.group(7):
            setattr(args, 'host', m.group(7))
        if m.group(9) is not None:
            setattr(args, 'port', int(m.group(9)))
        if m.group(11) is not None:
            setattr(args, 'docbaseid', int(m.group(11)))

    if args.login and not args.authentication:
        m = re.match('^(.*?):(.*)$', args.login)
        if m:
            setattr(args, 'login', m.group(1))
            setattr(args, 'authentication', m.group(2))

    check = Check(CheckSummary())
    if args.name:
        check.name = args.name
    else:
        check.name = args.mode
    check.add(CheckDocbase(args, check.results))
    check.add(
        ScalarContext(
            THRESHOLDS,
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


class CustomMetric(Metric):
    def add_message(self, message):
        self.message = message
        return self

    def replace(self, **attr):
        if hasattr(self, 'message'):
            return super(CustomMetric, self).replace(**attr).add_message(self.message)
        return super(CustomMetric, self).replace(**attr)


if __name__ == '__main__':
    main()