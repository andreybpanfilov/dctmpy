#!python
import argparse
import re

import nagiosplugin
from nagiosplugin.state import Critical, Warn, Ok, Unknown

from dctmpy.docbaseclient import DocbaseClient
from dctmpy.docbrokerclient import DocbrokerClient


JOB_ATTRIBUTES = ['object_name', 'is_inactive', 'a_last_invocation',
                  'a_last_completion', 'a_last_return_code', 'a_current_status',
                  'a_status', 'a_special_app', 'run_mode', 'run_interval',
                  'expiration_date', 'max_iterations', 'a_iterations',
                  'a_next_invocation', 'start_date', 'a_current_status']

JOB_QUERY = "SELECT " + ", ".join(JOB_ATTRIBUTES) + " FROM dm_job WHERE 1=1 "

JOB_ACTIVE_CONDITION = " AND ((a_last_invocation IS NOT NULLDATE and a_last_completion IS NULLDATE) " \
                       " OR a_special_app = 'agentexec')" \
                       " AND (i_is_reference = 0 OR i_is_reference is NULL)" \
                       " AND (i_is_replica = 0 OR i_is_replica is NULL)"

JOB_INTERVALS = {
    1: 60,
    2: 60 * 60,
    3: 24 * 60 * 60,
    4: 7 * 24 * 60 * 60
}


class CheckDocbase(nagiosplugin.Resource):
    def __init__(self, args, results):
        self.args = args
        self.results = results
        self.session = None

    def probe(self):
        yield nagiosplugin.Metric("null", 0, context='null')
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
        yield nagiosplugin.Metric('sessioncount', int(count['hot_list_size']), min=0,
                                  max=int(count['concurrent_sessions']),
                                  context='sessioncount')

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
        if session is not None:
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
                jobs_to_check = re.split(",\s*", self.jobs)
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
                if jobs_to_check is not None and job['object_name'] in jobs_to_check:
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
        if job['start_date'] is None or job['start_date'] <= 0:
            message = "%s has undefined start_date" % job['object_name']
            self.add_result(Critical, message)
            return
        if job['a_next_invocation'] is None or job['a_next_invocation'] <= 0:
            message = "%s has undefined next_invocation_date" % job['object_name']
            self.add_result(Critical, message)
            return
        if job['expiration_date'] is not None and job['expiration_date'] < job['start_date']:
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
        if job['expiration_date'] is not None and now > job['expiration_date']:
            message = "%s is expired" % job['object_name']
            self.add_result(Critical, message)
            return
        if 0 < job['max_iterations'] < job['a_iterations']:
            message = "%s max iterations exceeded" % job['object_name']
            self.add_result(Critical, message)
            return
        if job['a_last_invocation'] is None:
            message = "%s has been never executed" % job['object_name']
            self.add_result(Warn, message)
            return
        if job['a_last_return_code'] != 0:
            message = "%s has status: %s" % ((job['object_name']), (job['a_current_status']))
            self.add_result(Critical, message)
            return
        if re.search('agentexec', job['a_special_app']) is not None or (
                    job['a_last_invocation'] is not None and job['a_last_completion'] is None):
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
        ''

    def check_query(self):
        ''

    def check_count_query(self):
        ''

    def check_work_queue(self):
        query = "SELECT count(r_object_id) AS work_queue_size FROM dmi_workitem " \
                "WHERE r_runtime_state IN (0, 1) " \
                "AND r_auto_method_id > '0000000000000000' " \
                "AND a_wq_name is NULLSTRING"
        try:
            result = CheckDocbase.read_object(self.session, query)
            yield nagiosplugin.Metric('workqueue', int(result['work_queue_size']), min=0, context='workqueue')
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
            yield nagiosplugin.Metric(server_name[-20:], int(result['work_queue_size']), min=0,
                                      context='serverworkqueue')
        except Exception, e:
            message = "Unable to execute query: %s" % str(e)
            self.add_result(Critical, message)

    def check_fulltext_queue(self):
        try:
            count = 0
            for user in CheckDocbase.read_query(self.session,
                                                "select distinct queue_user from dm_ftindex_agent_config"):
                count += 1
                self.check_fulltext_queue(user['queue_user'])
            if count == 0:
                message = "No indexagents"
                self.add_result(Warn, message)
        except Exception, e:
            message = "Unable to execute query: %s" % str(e)
            self.add_result(Critical, message)

    def check_fulltext_queue(self, username):
        query = "SELECT count(r_object_id) AS queue_size FROM dmi_queue_item WHERE name='" \
                + username + "'AND task_state not in ('failed','warning')"
        try:
            result = CheckDocbase.read_object(self.session, query)
            yield nagiosplugin.Metric(username[-20:], int(result['queue_size']), min=0,
                                      context='indexqueue')
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
        self.results.add(nagiosplugin.Result(state, message))

    def __getattr__(self, name):
        if hasattr(self.args, name):
            return getattr(self.args, name)
        else:
            return AttributeError("Unknown attribute %s in %s" % (name, str(self.__class__)))

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
            if col is not None:
                col.close()

    @staticmethod
    def read_object(session, query):
        results = CheckDocbase.read_query(session, query, 1)
        if results:
            for rec in results:
                return rec

    @staticmethod
    def get_jobs(session, jobs=None, condition=""):
        query = JOB_QUERY + condition
        if jobs is not None:
            query += " AND object_name IN ('" + "','".join(jobs) + "')"
        return CheckDocbase.read_query(session, query)

    @staticmethod
    def get_running_jobs(session):
        return CheckDocbase.get_jobs(session, JOB_ACTIVE_CONDITION)

    @staticmethod
    def get_failed_tasks(session, offset=None):
        query = "SELECT que.task_name, que.name" \
                " FROM dmi_queue_item que, dmi_workitem wi, dmi_package pkg" \
                " WHERE que.event = 'dm_changedactivityinstancestate'" \
                " AND que.item_id LIKE '4%%'" \
                " AND que.MESSAGE LIKE 'Activity instance, %%, of workflow, %%, failed.'" \
                " AND que.item_id = wi.r_object_id" \
                " AND wi.r_workflow_id = pkg.r_workflow_id" \
                " AND wi.r_act_seqno = pkg.r_act_seqno" \
                " AND que.delete_flag = 0"
        if offset is not None:
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


class CheckSummary(nagiosplugin.Summary):
    def verbose(self, results):
        return ''

    def ok(self, results):
        return self.format(results)

    def problem(self, results):
        return self.format(results)

    def format(self, results):
        message = ""
        for state in [Ok, Unknown, Warn, Critical]:
            hint = ", ".join(str(x) for x in results if x.state == state and not CheckDocbase.is_empty(x.hint))
            message = ", ".join(x for x in [hint, message] if not (CheckDocbase.is_empty(x)))
        return message


modes = {
    'sessioncount': [CheckDocbase.check_sessions, True, "checks active session count"],
    'targets': [CheckDocbase.check_targets, False, "checks whether server is registered on projection targets"],
    'indexagents': [CheckDocbase.check_index_agents, False, "checks index agent status"],
    'check_jobs': [CheckDocbase.check_jobs, False, "checks jobs scheduling"],
    'timeskew': [CheckDocbase.check_time_skew, True, "checks time skew between nagios host and documentum"],
    'query': [CheckDocbase.check_query, True, "checks results returned by query"],
    'countquery': [CheckDocbase.check_count_query, True, "checks results returned by query"],
    'workqueue': [CheckDocbase.check_work_queue, True, "checks workqueue size"],
    'serverworkqueue': [CheckDocbase.check_server_work_queue, True, "checks server workqueue size"],
    'indexqueue': [CheckDocbase.check_fulltext_queue, True, "checks index agent queue size"],
    'failedtasks': [CheckDocbase.check_failed_tasks, True, "checks failed tasks"],
    'login': [CheckDocbase.check_login, False, "checks login"],
}


@nagiosplugin.guarded
def main():
    argp = argparse.ArgumentParser(description=__doc__)
    argp.add_argument('-H', '--host', required=True, metavar='hostname', help='server hostname')
    argp.add_argument('-p', '--port', required=True, metavar='port', type=int, help='server port')
    argp.add_argument('-i', '--docbaseid', required=True, metavar='docbaseid', type=int, help='docbase identifier')
    argp.add_argument('-l', '--login', metavar='username', help='username')
    argp.add_argument('-a', '--authentication', metavar='password', help='password')
    argp.add_argument('-t', '--timeout', metavar='timeout', default=60, type=int, help='check timeout')
    argp.add_argument('-m', '--mode', required=True, metavar='mode',
                      help="check to use, any of: " + "; ".join(x + " - " + modes[x][2] for x in modes.keys()))
    argp.add_argument('-j', '--jobs', metavar='jobs', default='', help='jobs to check')
    for mode in modes.keys():
        if not modes[mode][1]:
            continue
        argp.add_argument("--" + mode + "-warning", metavar='RANGE',
                          help='<warning range for ' + mode + ' check>')
        argp.add_argument("--" + mode + "-critical", metavar='RANGE',
                          help='<critical range for ' + mode + ' check>')
    args = argp.parse_args()
    check = nagiosplugin.Check(CheckSummary())
    check.name = args.mode
    check.add(CheckDocbase(args, check.results))
    for mode in modes.keys():
        if not modes[mode][1]:
            continue
        check.add(
            nagiosplugin.ScalarContext(mode, getattr(args, mode + "_warning"), getattr(args, mode + "_critical")))
    check.main(timeout=args.timeout)


if __name__ == '__main__':
    main()