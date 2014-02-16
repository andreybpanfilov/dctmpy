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
            self.checkLogin()
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

    def checkSessions(self):
        try:
            count = self.session.COUNT_SESSIONS()
        except Exception, e:
            self.addResult(Critical, "Unable to retrieve session count: " + str(e))
            return
        yield nagiosplugin.Metric('sessioncount', int(count['hot_list_size']), min=0,
                                  max=int(count['concurrent_sessions']),
                                  context='sessioncount')

    def checkTargets(self):
        targets = []
        servername = self.session.serverconfig['object_name']
        docbaseame = self.session.docbaseconfig['object_name']
        try:
            for target in self.session.LIST_TARGETS():
                targets.extend(zip(target['projection_targets'], target['projection_ports']))
        except Exception, e:
            message = "Unable to retrieve targets: %s" % str(e)
            self.addResult(Critical, message)
            return

        for (host, port) in targets:
            self.checkRegistration(host, port, docbaseame, servername)

    def checkRegistration(self, docbrokerhost, docbrokerport, docbaseame, servername):
        docbroker = DocbrokerClient(host=docbrokerhost, port=docbrokerport)

        try:
            docbasemap = docbroker.getDocbaseMap()
        except Exception, e:
            message = "Unable to retrieve docbasemap from docbroker %s:%d: %s" % (
                docbrokerhost, docbrokerport, str(e))
            self.addResult(Critical, message)
            return

        if not docbaseame in docbasemap['r_docbase_name']:
            message = "docbase %s is not registered on %s:%d" % (docbaseame, docbrokerhost, docbrokerport)
            self.addResult(Critical, message)
            return

        try:
            servermap = docbroker.getServerMap(docbaseame)
        except Exception, e:
            message = "Unable to retrieve servermap from docbroker %s:%d: %s" % (
                docbrokerhost, docbrokerport, str(e))
            self.addResult(Critical, message)
            return

        if not servername in servermap['r_server_name']:
            message = "server %s.%s is not registered on %s:%d" % (
                docbaseame, servername, docbrokerhost, docbrokerport)
            self.addResult(Critical, message)
            return

        index = servermap['r_server_name'].index(servername)
        status = servermap['r_last_status'][index]
        docbaseid = servermap['i_docbase_id'][index]
        connaddr = servermap['i_server_connection_address'][index]

        if status != "Open":
            message = "%s.%s has status %s on %s:%d, " % (
                docbaseame, servername, status, docbrokerhost, docbrokerport)
            self.addResult(Critical, message)
            return

        chunks = connaddr.split(" ")
        host = chunks[5]
        port = int(chunks[2], 16)

        session = None
        try:
            session = DocbaseClient(host=host, port=port, docbaseid=docbaseid)
            message = "%s.%s has status %s on %s:%d" % (
                docbaseame, servername, status, docbrokerhost, docbrokerport)
            self.addResult(Ok, message)
        except Exception, e:
            message = "%s.%s has status %s on %s:%d, but error occurred during connection to %s" % (
                docbaseame, servername, status, docbrokerhost, docbrokerport, str(e))
            self.addResult(Critical, message)
            return
        if session is not None:
            try:
                session.disconnect()
            except Exception, e:
                pass

    def checkIndexAgents(self):
        try:
            count = 0
            for index in CheckDocbase.getIndexes(self.session):
                count += 1
                self.checkIndexAgent(index['index_name'], index['object_name'])
            if count == 0:
                message = "No indexagents"
                self.addResult(Warn, message)
        except Exception, e:
            message = "Unable to execute query: %s" % str(e)
            self.addResult(Critical, message)

    def checkIndexAgent(self, indexname, agentname):
        try:
            result = self.session.FTINDEX_AGENT_ADMIN(
                indexname, agentname
            )
            status = result['status'][0]
            if status == 0:
                message = "Indexagent %s/%s is up and running" % (indexname, agentname)
                self.addResult(Ok, message)
            elif status == 100:
                message = "Indexagent %s/%s is stopped" % (indexname, agentname)
                self.addResult(Warn, message)
            elif status == 200:
                message = "A problem with indexagent %s/%s" % (indexname, agentname)
                self.addResult(Critical, message)
            else:
                message = "Indexagent %s/%s has unknown status" % (indexname, agentname)
                self.addResult(Unknown, message)
        except Exception, e:
            message = "Unable to get indexagent %s/%s status: %s" % (
                indexname, agentname, str(e))
            self.addResult(Critical, message)

    def checkJobs(self):
        jobstocheck = None
        if not CheckDocbase.isEmpty(self.jobs):
            if isinstance(self.jobs, list):
                jobstocheck = list(self.jobs)
            elif isinstance(self.jobs, str):
                jobstocheck = re.split(",\s*", self.jobs)
            else:
                raise RuntimeError("Wrong jobs argument")

        try:
            now = self.session.TIME()
        except Exception, e:
            message = "Unable to acquire current time: %s" % str(e)
            self.addResult(Critical, message)
            return

        try:
            for job in CheckDocbase.getJobs(self.session, jobstocheck):
                if jobstocheck is not None and job['object_name'] in jobstocheck:
                    jobstocheck.remove(job['object_name'])
                self.checkJob(job, now)
        except Exception, e:
            message = "Unable to execute query: %s" % str(e)
            self.addResult(Critical, message)
            return

        if not CheckDocbase.isEmpty(jobstocheck):
            message = ""
            for job in jobstocheck:
                message += "%s not found, " % job
            self.addResult(Critical, message)

    def checkJob(self, job, now):
        if job['start_date'] is None or job['start_date'] <= 0:
            message = "%s has undefined start_date" % job['object_name']
            self.addResult(Critical, message)
            return
        if job['a_next_invocation'] is None or job['a_next_invocation'] <= 0:
            message = "%s has undefined next_invocation_date" % job['object_name']
            self.addResult(Critical, message)
            return
        if job['expiration_date'] is not None and job['expiration_date'] < job['start_date']:
            message = "%s has expiration_date less then start_date" % job['object_name']
            self.addResult(Critical, message)
            return
        if job['max_iterations'] < 0:
            message = "%s has invalid max_iterations value: %d" % (
                (job['object_name']), (job['max_iterations']))
            self.addResult(Critical, message)
            return
        if job['run_mode'] == 0 and job['run_interval'] == 0 and job['max_iterations'] != 1:
            message = "%s has invalid max_iterations value for run_mode=0 and run_interval=0" % job[
                'object_name']
            self.addResult(Critical, message)
            return
        if job['run_mode'] in [1, 2, 3, 4] and not (1 <= job['run_interval'] <= 32767):
            message = "%s has invalid run_interval value, expected [1, 32767], got %d" % (
                (job['object_name']), (job['run_interval']))
            self.addResult(Critical, message)
            return
        if job['run_mode'] == 7 and not (-7 <= job['run_interval'] <= 7 and job['run_interval'] != 0):
            message = "%s has invalid run_interval value, expected [-7,0) U (0,7], got %d" % (
                (job['object_name']), (job['run_interval']))
            self.addResult(Critical, message)
            return
        if job['run_mode'] == 8 and not (-28 <= job['run_interval'] <= 28 and job['run_interval'] != 0):
            message = "%s has invalid run_interval value, expected [-28,0) U (0,28], got %d" % (
                (job['object_name']), (job['run_interval']))
            self.addResult(Critical, message)
            return
        if job['run_mode'] == 9 and not (-365 <= job['run_interval'] <= 365 and job['run_interval'] != 0):
            message = "%s has invalid run_interval value, expected [-365,0) U (0,365], got %d" % (
                (job['object_name']), (job['run_interval']))
            self.addResult(Critical, message)
            return
        if job['is_inactive']:
            message = "%s is inactive" % job['object_name']
            self.addResult(Critical, message)
            return
        if job['expiration_date'] is not None and now > job['expiration_date']:
            message = "%s is expired" % job['object_name']
            self.addResult(Critical, message)
            return
        if 0 < job['max_iterations'] < job['a_iterations']:
            message = "%s max iterations exceeded" % job['object_name']
            self.addResult(Critical, message)
            return
        if job['a_last_invocation'] is None:
            message = "%s has been never executed" % job['object_name']
            self.addResult(Warn, message)
            return
        if job['a_last_return_code'] != 0:
            message = "%s has status: %s" % ((job['object_name']), (job['a_current_status']))
            self.addResult(Critical, message)
            return
        if re.search('agentexec', job['a_special_app']) is not None or (
                    job['a_last_invocation'] is not None and job['a_last_completion'] is None):
            message = "%s is running for %s" % (
                (job['object_name']), CheckDocbase.prettyInterval(now - job['a_last_invocation']))
            self.addResult(Ok, message)
            return

        timegap = now - job['a_last_completion']

        if job['run_mode'] in [1, 2, 3, 4]:
            message = "%s last run - %s ago" % ((job['object_name']), CheckDocbase.prettyInterval(timegap))
            if timegap > 2 * JOB_INTERVALS[job['run_mode']] * job['run_interval']:
                self.addResult(Critical, message)
                return
            elif timegap > JOB_INTERVALS[job['run_mode']] * job['run_interval']:
                self.addResult(Warn, message)
                return
            else:
                self.addResult(Ok, message)
                return
        else:
            message = "Scheduling type for job %s is not currently supported" % job['object_name']
            self.addResult(Critical, message)
            return

    def checkTimeSkew(self):
        ''

    def checkQuery(self):
        ''

    def checkCountQuery(self):
        ''

    def checkWorkQueue(self):
        query = "SELECT count(r_object_id) AS work_queue_size FROM dmi_workitem " \
                "WHERE r_runtime_state IN (0, 1) " \
                "AND r_auto_method_id > '0000000000000000' " \
                "AND a_wq_name is NULLSTRING"
        try:
            result = CheckDocbase.readObject(self.session, query)
            yield nagiosplugin.Metric('workqueue', int(result['work_queue_size']), min=0, context='workqueue')
        except Exception, e:
            message = "Unable to execute query: %s" % str(e)
            self.addResult(Critical, message)

    def checkServerWorkQueue(self):
        serverid = self.session.serverconfig['r_object_id']
        servername = self.session.serverconfig['object_name']
        query = "SELECT count(r_object_id) AS work_queue_size FROM dmi_workitem " \
                "WHERE r_runtime_state IN (0, 1) " \
                "AND r_auto_method_id > '0000000000000000' " \
                "AND a_wq_name ='" + serverid + "'"
        try:
            result = CheckDocbase.readObject(self.session, query)
            yield nagiosplugin.Metric(servername[-20:], int(result['work_queue_size']), min=0,
                                      context='serverworkqueue')
        except Exception, e:
            message = "Unable to execute query: %s" % str(e)
            self.addResult(Critical, message)

    def checkFulltextQueue(self):
        try:
            count = 0
            for user in CheckDocbase.readQuery(self.session, "select distinct queue_user from dm_ftindex_agent_config"):
                count += 1
                self.checkFulltextQueue(user['queue_user'])
            if count == 0:
                message = "No indexagents"
                self.addResult(Warn, message)
        except Exception, e:
            message = "Unable to execute query: %s" % str(e)
            self.addResult(Critical, message)

    def checkFulltextQueue(self, username):
        query = "SELECT count(r_object_id) AS queue_size FROM dmi_queue_item WHERE name='" \
                + username + "'AND task_state not in ('failed','warning')"
        try:
            result = CheckDocbase.readObject(self.session, query)
            yield nagiosplugin.Metric(username[-20:], int(result['queue_size']), min=0,
                                      context='indexqueue')
        except Exception, e:
            message = "Unable to execute query: %s" % str(e)
            self.addResult(Critical, message)

    def checkFailedTasks(self):
        try:
            count = 0
            message = ""
            for rec in CheckDocbase.getFailedTasks(self.session):
                count += 1
                if count > 1:
                    message += ", "
                message += "'%s' (%s)" % (rec['task_name'], rec['name'])

            if count > 0:
                message = "%d task(s): %s" % (count, message)
                self.addResult(Critical, message)
        except Exception, e:
            message = "Unable to execute query: %s" % str(e)
            self.addResult(Critical, message)

    def checkLogin(self):
        try:
            session = DocbaseClient(host=self.host, port=self.port, docbaseid=self.docbaseid)
        except Exception, e:
            message = "Unable to connect to docbase: %s" % str(e)
            self.addResult(Critical, message)
            return

        if self.login and self.authentication:
            try:
                session.authenticate(self.login, self.authentication)
            except Exception, e:
                message = "Unable to authenticate: %s" % str(e)
                self.addResult(Critical, message)
                try:
                    session.disconnect()
                except Exception, e:
                    pass
                return
            self.session = session
        else:
            message = ["No username provided", "No password provided"][not self.authentication]
            status = [Warn, Critical][self.mode != 'login']
            self.addResult(status, message)

    def addResult(self, state, message):
        self.results.add(nagiosplugin.Result(state, message))

    def __getattr__(self, name):
        if hasattr(self.args, name):
            return getattr(self.args, name)
        else:
            return AttributeError

    @staticmethod
    def getIndexes(session):
        query = "select index_name, a.object_name " \
                "from dm_fulltext_index i, dm_ftindex_agent_config a " \
                "where i.index_name=a.index_name " \
                "and a.force_inactive = false"
        return CheckDocbase.readQuery(session, query)

    @staticmethod
    def readQuery(session, query, cnt=0):
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
    def readObject(session, query):
        results = CheckDocbase.readQuery(session, query, 1)
        if results:
            for rec in results:
                return rec

    @staticmethod
    def getJobs(session, jobs=None, condition=""):
        query = JOB_QUERY + condition
        if jobs is not None:
            query += " AND object_name IN ('" + "','".join(jobs) + "')"
        return CheckDocbase.readQuery(session, query)

    @staticmethod
    def getRunningJobs(session):
        return CheckDocbase.getJobs(session, JOB_ACTIVE_CONDITION)

    @staticmethod
    def getFailedTasks(session, offset=None):
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
        return CheckDocbase.readQuery(session, query)


    @staticmethod
    def prettyInterval(delta):
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
    def isEmpty(value):
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
            hint = ", ".join(str(x) for x in results if x.state == state and not CheckDocbase.isEmpty(x.hint))
            message = ", ".join(x for x in [hint, message] if not (CheckDocbase.isEmpty(x)))
        return message


modes = {
    'sessioncount': [CheckDocbase.checkSessions, True, "checks active session count"],
    'targets': [CheckDocbase.checkTargets, False, "checks whether server is registered on projection targets"],
    'indexagents': [CheckDocbase.checkIndexAgents, False, "checks index agent status"],
    'checkjobs': [CheckDocbase.checkJobs, False, "checks jobs scheduling"],
    'timeskew': [CheckDocbase.checkTimeSkew, True, "checks time skew between nagios host and documentum"],
    'query': [CheckDocbase.checkQuery, True, "checks results returned by query"],
    'countquery': [CheckDocbase.checkCountQuery, True, "checks results returned by query"],
    'workqueue': [CheckDocbase.checkWorkQueue, True, "checks workqueue size"],
    'serverworkqueue': [CheckDocbase.checkServerWorkQueue, True, "checks server workqueue size"],
    'indexqueue': [CheckDocbase.checkFulltextQueue, True, "checks index agent queue size"],
    'failedtasks': [CheckDocbase.checkFailedTasks, True, "checks failed tasks"],
    'login': [CheckDocbase.checkLogin, False, "checks login"],
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