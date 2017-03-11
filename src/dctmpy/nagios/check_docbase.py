#!/usr/bin/env python

import argparse
import re
import time

from dctmpy import get_current_time_mills
from dctmpy.nagios import *

try:
    from urllib.request import urlopen, URLError
except ImportError:
    from urllib2 import urlopen, URLError

try:
    from urllib import urlencode
except ImportError:
    urlencode = None

from xml.dom import minidom

from nagiosplugin import Metric, Result, Check, Resource, guarded, ScalarContext
from nagiosplugin.state import Critical, Warn, Ok, Unknown

from dctmpy.docbaseclient import DocbaseClient
from dctmpy.docbrokerclient import DocbrokerClient


class CheckDocbase(Resource):
    def __init__(self, args, results):
        self.args = args
        self.results = results
        self.session = None

    def probe(self):
        yield Metric(NULL_CONTEXT, 0, context=NULL_CONTEXT)
        try:
            for result in self.check_login(self.mode == 'login'):
                if result:
                    yield result
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
            except:
                pass

    def check_sessions(self):
        try:
            count = self.session.count_sessions()
        except Exception, e:
            self.add_result(Critical, "Unable to retrieve session count: %s" % str(e))
            return
        yield Metric('sessioncount', int(count['hot_list_size']),
                     min=0, max=int(count['concurrent_sessions']),
                     context=THRESHOLDS)

    def check_uptime(self):
        try:
            start = self.session.list_sessions()['root_start']
            uptime = self.session.time() - start
        except Exception, e:
            self.add_result(Critical, "Unable to retrieve session list: %s" % str(e))
            return
        yield CustomMetric('uptime', int(uptime), "s", min=0, context=THRESHOLDS) \
            .add_message("CS uptime is %s" % pretty_interval(uptime))

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
        docbroker = DocbrokerClient(host=docbrokerhost, port=docbrokerport + [0, 1][self.args.secure],
                                    secure=self.args.secure, ciphers=CIPHERS)

        try:
            docbasemap = docbroker.get_docbase_map()
        except Exception, e:
            message = "Unable to retrieve docbasemap from docbroker %s:%d: %s" % \
                      (docbrokerhost, docbrokerport, str(e))
            self.add_result(Critical, message)
            return

        if docbaseame not in docbasemap['r_docbase_name']:
            message = "docbase %s is not registered on %s:%d" % \
                      (docbaseame, docbrokerhost, docbrokerport)
            self.add_result(Critical, message)
            return

        try:
            servermap = docbroker.get_server_map(docbaseame)
        except Exception, e:
            message = "Unable to retrieve servermap from docbroker %s:%d: %s" % \
                      (docbrokerhost, docbrokerport, str(e))
            self.add_result(Critical, message)
            return

        if servername not in servermap['r_server_name']:
            message = "server %s.%s is not registered on %s:%d" % \
                      (docbaseame, servername, docbrokerhost, docbrokerport)
            self.add_result(Critical, message)
            return

        index = servermap['r_server_name'].index(servername)
        status = servermap['r_last_status'][index]
        docbaseid = servermap['i_docbase_id'][index]
        connaddr = servermap['i_server_connection_address'][index]

        if status != "Open":
            message = "%s.%s has status %s on %s:%d, " % \
                      (docbaseame, servername, status,
                       docbrokerhost, docbrokerport)
            self.add_result(Critical, message)
            return

        chunks = connaddr.split(" ")
        host = chunks[5]
        port = int(chunks[2], 16)

        session = None
        try:
            session = DocbaseClient(host=host, port=port + [0, 1][self.args.secure],
                                    docbaseid=docbaseid, secure=self.args.secure,
                                    ciphers=CIPHERS)
        except Exception, e:
            message = "%s.%s has status %s on %s:%d, " \
                      "but error occurred whilst connecting to %s" % \
                      (docbaseame, servername, status,
                       docbrokerhost, docbrokerport, str(e))
            self.add_result(Critical, message)
            return
        if session:
            message = "%s.%s has status %s on %s:%d" % \
                      (docbaseame, servername, status,
                       docbrokerhost, docbrokerport)
            self.add_result(Ok, message)
            try:
                session.disconnect()
            except:
                pass

    def check_index_agents(self):
        try:
            count = 0
            for index in get_indexes(self.session):
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
                message = "Indexagent %s/%s is up and running" % \
                          (index_name, agent_name)
                self.add_result(Ok, message)
            elif status == 100:
                message = "Indexagent %s/%s is stopped" % \
                          (index_name, agent_name)
                self.add_result(Warn, message)
            elif status == 200:
                message = "A problem with indexagent %s/%s" % \
                          (index_name, agent_name)
                self.add_result(Critical, message)
            else:
                message = "Indexagent %s/%s has unknown status" % \
                          (index_name, agent_name)
                self.add_result(Unknown, message)
        except Exception, e:
            message = "Unable to get indexagent %s/%s status: %s" % \
                      (index_name, agent_name, str(e))
            self.add_result(Critical, message)

    def check_jobs(self):
        jobs_to_check = None
        if not is_empty(self.jobs):
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
            for job in get_jobs(self.session, jobs_to_check):
                if jobs_to_check and job['object_name'] in jobs_to_check:
                    jobs_to_check.remove(job['object_name'])
                result = self.check_job(job, now)
                self.add_result(result[0], result[1])
        except Exception, e:
            message = "Unable to execute query: %s" % str(e)
            self.add_result(Critical, message)
            return

        if not is_empty(jobs_to_check):
            message = ""
            for job in jobs_to_check:
                message += "%s not found, " % job
            self.add_result(Critical, message)

    def check_no_jobs(self):
        jobs_to_check = None
        if not is_empty(self.jobs):
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
            for job in get_jobs(self.session, jobs_to_check):
                if jobs_to_check and job['object_name'] in jobs_to_check:
                    jobs_to_check.remove(job['object_name'])
                result = self.check_job(job, now, False)
                (status, message, _) = result
                if status == Critical or status == Warn:
                    status = Ok
                    message = result[1]
                elif status == Ok:
                    status = Critical
                    message = "%s is active" % job['object_name']
                self.add_result(status, message)
        except Exception, e:
            message = "Unable to execute query: %s" % str(e)
            self.add_result(Critical, message)
            return

        if not is_empty(jobs_to_check):
            message = ""
            for job in jobs_to_check:
                message += "%s not found, " % job
            self.add_result(Unknown, message)

    def check_job(self, job, now, check_scheduling=True):
        if not job['start_date'] > -1:
            message = "%s has undefined start_date" % \
                      job['object_name']
            return Result(Critical, message)
        if not job['a_next_invocation'] > -1:
            message = "%s has undefined next_invocation_date" % \
                      job['object_name']
            return Result(Critical, message)
        if -1 < job['expiration_date'] < job['start_date']:
            message = "%s has expiration_date less then start_date" % \
                      job['object_name']
            return Result(Critical, message)
        if job['max_iterations'] < 0:
            message = "%s has invalid max_iterations value: %d" % \
                      ((job['object_name']), (job['max_iterations']))
            return Result(Critical, message)
        if job['run_mode'] == 0 and job['run_interval'] == 0 \
                and job['max_iterations'] != 1:
            message = "%s has invalid max_iterations value for " \
                      "run_mode=0 and run_interval=0" % \
                      job['object_name']
            return Result(Critical, message)
        if 1 <= job['run_mode'] <= 4 \
                and not (1 <= job['run_interval'] <= 32767):
            message = "%s has invalid run_interval value, " \
                      "expected [1, 32767], got %d" % \
                      (job['object_name'], job['run_interval'])
            return Result(Critical, message)
        if job['run_mode'] == 7 \
                and not (-7 <= job['run_interval'] <= 7
                         and job['run_interval'] != 0):
            message = "%s has invalid run_interval value, " \
                      "expected [-7,0) U (0,7], got %d" % \
                      (job['object_name'], job['run_interval'])
            return Result(Critical, message)
        if job['run_mode'] == 8 \
                and not (-28 <= job['run_interval'] <= 28
                         and job['run_interval'] != 0):
            message = "%s has invalid run_interval value, " \
                      "expected [-28,0) U (0,28], got %d" % \
                      (job['object_name'], job['run_interval'])
            return Result(Critical, message)
        if job['run_mode'] == 9 \
                and not (-365 <= job['run_interval'] <= 365
                         and job['run_interval'] != 0):
            message = "%s has invalid run_interval value, " \
                      "expected [-365,0) U (0,365], got %d" % \
                      (job['object_name'], job['run_interval'])
            return Result(Critical, message)
        if job['is_inactive']:
            message = "%s is inactive" % job['object_name']
            return Result(Critical, message)
        if now > job['expiration_date'] > -1:
            message = "%s is expired" % job['object_name']
            return Result(Critical, message)
        if 0 < job['max_iterations'] < job['a_iterations']:
            message = "%s max iterations exceeded" % job['object_name']
            return Result(Critical, message)
        if not job['a_last_invocation'] > -1:
            message = "%s has been never executed" % job['object_name']
            return Result(Warn, message)

        time_diff = now - job['a_last_completion']

        if not check_scheduling:
            message = "%s last run - %s ago" % \
                      ((job['object_name']), pretty_interval(time_diff))
            return Result(Ok, message)

        if job['a_last_return_code'] != 0:
            message = "%s has status: %s" % \
                      ((job['object_name']), (job['a_current_status']))
            return Result(Critical, message)
        if re.match('agentexec', job['a_special_app']) or \
                (job['a_last_invocation'] > -1 and not job['a_last_completion']):
            message = "%s is running for %s" % \
                      (job['object_name'], pretty_interval(now - job['a_last_invocation']))
            return Result(Ok, message)

        if 1 <= job['run_mode'] <= 4:
            period = JOB_INTERVALS[job['run_mode']] * job['run_interval']
            message = "%s last run - %s ago" % \
                      ((job['object_name']), pretty_interval(time_diff))
            if time_diff > [self.args.critical, 2][self.args.critical is None] * period:
                return Result(Critical, message)
            if time_diff > [self.args.warning, 1][self.args.warning is None] * period:
                return Result(Warn, message)
            return Result(Ok, message)
        message = "Scheduling type for job %s is not currently supported" % \
                  job['object_name']
        return Result(Unknown, message)

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
            start = get_current_time_mills()
            result = read_object(self.session, self.query)
            yield Metric('query_time', get_current_time_mills() - start, "ms", min=0, context=TIME_THRESHOLDS)
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
            start = get_current_time_mills()
            end = None
            for rec in read_query(self.session, self.query):
                if not end:
                    end = get_current_time_mills()
                count += 1
                if count > 1:
                    message += ", "
                message += self.format.format(**rec)
            if not end:
                end = get_current_time_mills()
            message += " - %sms" % (end - start)
            yield CustomMetric('count', count, min=0, context=THRESHOLDS).add_message(message)
            yield Metric('query_time', end - start, "ms", min=0, context=TIME_THRESHOLDS)
        except Exception, e:
            message = "Unable to execute query: %s" % str(e)
            self.add_result(Critical, message)

    def check_count_query(self):
        try:
            start = get_current_time_mills()
            result = read_object(self.session, self.query)
            yield Metric('query_time', get_current_time_mills() - start, "ms", min=0, context=TIME_THRESHOLDS)
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
            result = read_object(self.session, query)
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
                "AND a_wq_name ='%s'" % server_id
        try:
            result = read_object(self.session, query)
            yield Metric(server_name, int(result['work_queue_size']), min=0, context=THRESHOLDS)
        except Exception, e:
            message = "Unable to execute query: %s" % str(e)
            self.add_result(Critical, message)

    def check_fulltext_queue(self):
        query = "select distinct queue_user from dm_ftindex_agent_config"
        try:
            count = 0
            for user in read_query(self.session, query):
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
        query = "SELECT count(r_object_id) AS queue_size FROM dmi_queue_item" \
                " WHERE name='%s' AND task_state not in ('failed','warning')" % username
        try:
            result = read_object(self.session, query)
            return Metric(username, int(result['queue_size']), min=0, context=THRESHOLDS)
        except Exception, e:
            message = "Unable to execute query: %s" % str(e)
            self.add_result(Critical, message)

    def check_cts_queue(self):
        query = "SELECT count(r_object_id) AS queue_size " \
                "FROM dmi_queue_item WHERE name='dm_mediaserver' AND delete_flag=FALSE"
        try:
            result = read_object(self.session, query)
            yield Metric("dm_mediaserver", int(result['queue_size']), min=0, context=THRESHOLDS)
        except Exception, e:
            message = "Unable to execute query: %s" % str(e)
            self.add_result(Critical, message)

    def check_failed_tasks(self):
        try:
            count = 0
            message = ""
            for rec in get_failed_tasks(self.session):
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
        if 'DUMP_JMS_CONFIG_LIST' not in self.session.entrypoints:
            self.check_jms_status_old()
        else:
            for metric in self.check_jms_status_new():
                if metric:
                    yield metric

    def check_jms_status_new(self):
        try:
            config = self.session.dump_jms_config_list()
            for c in xrange(0, len(config['jms_config_id'])):
                if config['is_disabled_in_docbase'][c]:
                    message = "%s is disabled" % config['jms_config_name'][c]
                    self.add_result(Warn, message)
                    continue
                if config['is_marked_dead_in_cache'][c]:
                    message = "%s is marked as dead" % config['jms_config_name'][c]
                    self.add_result(Critical, message)
                else:
                    try:
                        jmsconfig = self.session.get_object(config['jms_config_id'][c])
                        for s in xrange(0, len(jmsconfig['servlet_name'])):
                            name = jmsconfig['servlet_name'][s]
                            url = jmsconfig['base_uri'][s]
                            for metric in self.check_app_server(name, url, "%s_%s" % (jmsconfig['r_object_id'], name)):
                                if metric:
                                    yield metric
                    except Exception, e:
                        message = "Unable to retrieve jms config %s: %s" % (config['jms_config_id'][c], str(e))
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
            self.check_app_server(name, url, "%s_%s" % (serverconfig['object_name'], name))

    def check_acs_status(self):
        try:
            count = 0
            for rec in get_acs_configs(self.session):
                count += 1
                try:
                    acs = self.session.get_object(rec['r_object_id'])
                    for i in xrange(0, len(acs['acs_base_url'])):
                        url = acs['acs_base_url'][i]
                        name = "%s_%s" % (acs['r_object_id'], i)
                        for metric in self.check_app_server('acs', url, name):
                            if metric:
                                yield metric
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
            for rec in get_xplore_configs(self.session):
                count += 1
                try:
                    xplore = self.session.get_object(rec['r_object_id'])
                    prop = dict(zip(xplore['param_name'], xplore['param_value']))
                    url = "%s://%s:%s%s" % \
                          (prop['dsearch_qrserver_protocol'].lower(),
                           prop['dsearch_qrserver_host'],
                           prop['dsearch_qrserver_port'],
                           prop['dsearch_qrserver_target'])
                    for metric in self.check_app_server('dsearch', url, xplore['r_object_id']):
                        if metric:
                            yield metric
                except Exception, e:
                    message = "Unable to retrieve ft_engine config %s: %s" % \
                              (rec['r_object_id'], str(e))
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
            for cts in get_cts_instances(self.session):
                count += 1
                for metric in self.check_cts(cts, cts['r_object_id']):
                    if metric:
                        yield metric
            if count == 0:
                message = "No CTS instances"
                self.add_result(Warn, message)
        except Exception, e:
            message = "Unable to execute query: %s" % str(e)
            self.add_result(Critical, message)
            return

    def check_cts(self, cts, metric_name):
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
            start = get_current_time_mills()
            if urlencode:
                response = urlopen(url, data=urlencode(data), timeout=APP_SERVER_TIMEOUT)
            else:
                response = urlopen(url, data=data, timeout=APP_SERVER_TIMEOUT)
            if response.code != 200:
                message = "Unable to open %s (instance: %s): response code %d" % \
                          (url, instance_name, response.code)
                self.add_result(Critical, message)
                return

            yield Metric('response_time_%s' % metric_name,
                         get_current_time_mills() - start, "ms", min=0, context=TIME_THRESHOLDS)

            try:
                xmldoc = minidom.parseString(response.read())
                status = xmldoc.getElementsByTagName("message-id")[0].firstChild.nodeValue
                message = "CTS Agent for instance \"%s\" has status \"%s\"" % \
                          (instance_name, status)
                if status != "RUNNING":
                    self.add_result(Critical, message)
                else:
                    self.add_result(Ok, message)
            except Exception, e:
                message = "Unable to parse xml response (instance: %s): %s" % \
                          (instance_name, str(e))
                self.add_result(Critical, message)

        except URLError, e:
            message = "Unable to open %s (instance: %s): %s" % \
                      (url, instance_name, e.reason)
            self.add_result(Critical, message)
        except Exception, e:
            message = "Unable to open %s (instance: %s): %s" % \
                      (url, instance_name, str(e))
            self.add_result(Critical, message)

    def check_app_server(self, name, url, metric_name):
        if name not in APP_SERVER_RESPONSES:
            return
        try:
            serverconfig = self.session.serverconfig
            url = re.sub(r'(https?://)(localhost(\.localdomain)?|127\.0\.0\.1)([:/])?',
                         r'\1' + serverconfig['r_host_name'] + r'\4', url)

            start = get_current_time_mills()
            response = urlopen(url, timeout=APP_SERVER_TIMEOUT)
            if not (response.code >= 200 and response < 300):
                message = "Unable to open %s: response code %d" % \
                          (url, response.code)
                self.add_result(Critical, message)
                return

            end = get_current_time_mills()
            yield Metric('response_time_%s' % metric_name,
                         end - start, "ms", min=0, context=TIME_THRESHOLDS)

            expected = APP_SERVER_RESPONSES[name]
            if expected not in response.read():
                message = "text \"%s\" not found in response from %s" % \
                          (expected, url)
                self.add_result(Critical, message)
                return

            message = "%s - %sms" % (url, end - start)
            self.add_result(Ok, message)

        except Exception, e:
            message = "Unable to open %s: %s" % (url, str(e))
            self.add_result(Critical, message)

    def check_login(self, yield_metrics=False):
        message = ""
        try:
            start = get_current_time_mills()
            session = DocbaseClient(host=self.host, port=self.port, docbaseid=self.docbaseid, secure=self.secure,
                                    ciphers=CIPHERS)
            if yield_metrics:
                end = get_current_time_mills()
                message += "connection: %sms" % (end - start)
                yield Metric('connection_time', end - start, "ms", min=0, context=TIME_THRESHOLDS)
        except Exception, e:
            message = "Unable to connect to docbase: %s" % str(e)
            self.add_result(Critical, message)
            return

        if self.login and self.authentication:
            try:
                message = ("user: %s, " % self.login) + message
                start = get_current_time_mills()
                session.authenticate(self.login, self.authentication)
                if yield_metrics:
                    end = get_current_time_mills()
                    message += ", authentication: %sms" % (end - start)
                    yield Metric('authentication_time', get_current_time_mills() - start, "ms", min=0,
                                 context=TIME_THRESHOLDS)
            except Exception, e:
                message = "failed to authenticate as user %s: %s" % \
                          (self.login, str(e))
                self.add_result(Critical, message)
                try:
                    session.disconnect()
                except:
                    pass
                return
            self.session = session
            if yield_metrics:
                self.add_result(Ok, message)
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


modes = {
    'sessioncount': [CheckDocbase.check_sessions, True, False, "check active session count"],
    'uptime': [CheckDocbase.check_uptime, True, False, "check content server uptime"],
    'targets': [CheckDocbase.check_targets, False, False, "check whether server is registered on projection targets"],
    'indexagents': [CheckDocbase.check_index_agents, False, False, "check index agent status"],
    'jobs': [CheckDocbase.check_jobs, False, False, "check job scheduling"],
    'nojobs': [CheckDocbase.check_no_jobs, False, False, "check job scheduling"],
    'timeskew': [CheckDocbase.check_time_skew, True, False, "check time skew between nagios host and documentum"],
    'query': [CheckDocbase.check_query, True, True, "check results returned by query"],
    'method': [CheckDocbase.check_method, True, True, "check execution of method"],
    'countquery': [CheckDocbase.check_count_query, True, True, "check results returned by query"],
    'workqueue': [CheckDocbase.check_work_queue, True, False, "check workqueue size"],
    'serverworkqueue': [CheckDocbase.check_server_work_queue, True, False, "check server workqueue size"],
    'indexqueue': [CheckDocbase.check_fulltext_queue, True, False, "check index agent queue size"],
    'ctsqueue': [CheckDocbase.check_cts_queue, True, False, "check CTS agent queue size"],
    'failedtasks': [CheckDocbase.check_failed_tasks, True, False, "check failed tasks"],
    'login': [CheckDocbase.check_login, False, True, "check login"],
    'jmsstatus': [CheckDocbase.check_jms_status, False, True, "check JMS connectivity"],
    'ctsstatus': [CheckDocbase.check_cts_status, False, True, "check CTS connectivity"],
    'acsstatus': [CheckDocbase.check_acs_status, False, True, "check ACS connectivity"],
    'xplorestatus': [CheckDocbase.check_xplore_status, False, True, "check xPlore connectivity"],
}


@guarded
def main():
    argp = argparse.ArgumentParser(description=__doc__)
    argp.add_argument('-H', '--host', required=True, metavar='hostname', help='server hostname')
    argp.add_argument('-p', '--port', required=False, metavar='port', type=int, help='server port')
    argp.add_argument('-i', '--docbaseid', required=False, metavar='docbaseid', type=int, help='docbase identifier')
    argp.add_argument('-l', '--login', metavar='username', help='username')
    argp.add_argument('-a', '--authentication', metavar='password', help='password')
    argp.add_argument('-s', '--secure', action='store_true', help='use ssl')
    argp.add_argument('-t', '--timeout', metavar='timeout', default=60, type=int,
                      help='check timeout, default is 60 seconds')
    argp.add_argument('-m', '--mode', required=True, metavar='mode',
                      help="mode to use, one of: " + ", ".join("%s (%s)" % (x, modes[x][3]) for x in modes.keys()))
    argp.add_argument('-j', '--jobs', metavar='jobs', default='', help='jobs to check, comma-separated list')
    argp.add_argument('-n', '--name', metavar='name', default='', help='name of check that appears in output')
    argp.add_argument('-q', '--query', metavar='query', default='', help='query to run')
    argp.add_argument('-f', '--format', metavar='format', default='', help='query output format')
    argp.add_argument('-w', '--warning', metavar='RANGE',
                      help='warning threshold for query results, supported in following modes: ' + ", ".join(
                          x for x in modes.keys() if modes[x][1]))
    argp.add_argument('-c', '--critical', metavar='RANGE',
                      help='critical threshold for query results, supported in following modes: ' + ", ".join(
                          x for x in modes.keys() if modes[x][1]))
    argp.add_argument('--warningtime', metavar='RANGE',
                      help='warning threshold for execution time, supported in following modes: ' + ", ".join(
                          x for x in modes.keys() if modes[x][2]))
    argp.add_argument('--criticaltime', metavar='RANGE',
                      help='critical threshold for execution time, supported in following modes: ' + ", ".join(
                          x for x in modes.keys() if modes[x][2]))
    args = argp.parse_args()

    m = re.match('^(dctm(s)?://((.*?)(:(.*))?@)?)?([^/:]+?)(:(\d+))?(/(\d+))?$', args.host)
    if m:
        if m.group(2):
            setattr(args, 'secure', True)
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
    check.add(
        ScalarContext(
            TIME_THRESHOLDS,
            getattr(args, "warningtime"),
            getattr(args, "criticaltime"),
            fmt_metric=fmt_nometric,
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


def fmt_nometric(metric, context):
    return None


if __name__ == '__main__':
    main()
