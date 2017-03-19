from nagiosplugin import Summary, Ok, Unknown, Warn, Critical, Metric

CIPHERS = "ALL:aNULL:!eNULL"

THRESHOLDS = 'thresholds'
TIME_THRESHOLDS = 'time_thresholds'
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


def pretty_interval(delta):
    if delta >= 0:
        secs = delta % 60
        mins = (int(delta / 60)) % 60
        hours = (int(delta / 3600))
        if hours < 24:
            return "%02d:%02d:%02d" % (hours, mins, secs)
        else:
            days = int(hours / 24)
            hours -= days * 24
            return "%d days %02d:%02d:%02d" % (days, hours, mins, secs)
    return "future"


def get_failed_tasks(session, offset=None):
    query = "SELECT que.task_name, que.name " \
            "FROM dmi_queue_item que, dmi_workitem wi, dmi_package pkg " \
            "WHERE que.event = 'dm_changedactivityinstancestate' " \
            "AND que.item_id LIKE '4a%%' " \
            "AND que.message LIKE 'Activity instance, %%, of workflow, %%, failed.' " \
            "AND que.item_id = wi.r_object_id " \
            "AND wi.r_workflow_id = pkg.r_workflow_id " \
            "AND wi.r_act_seqno = pkg.r_act_seqno " \
            "AND que.delete_flag = 0"
    if offset >= 0:
        query += " que.date_sent > date(now) - %d " % offset
    return read_query(session, query)


def get_running_jobs(session):
    return get_jobs(session, JOB_ACTIVE_CONDITION)


def get_cts_instances(session, cts_names=None, condition=""):
    query = CTS_QUERY
    if is_empty(condition):
        if cts_names:
            query += " WHERE object_name IN ('" + "','".join(cts_names) + "')"
    else:
        query += " WHERE (%s)" % condition
        if cts_names:
            query += " AND object_name IN ('" + "','".join(cts_names) + "')"
    return read_query(session, query)


def get_jobs(session, jobs=None, condition=""):
    query = JOB_QUERY
    if is_empty(condition):
        if jobs:
            query += " WHERE object_name IN ('" + "','".join(jobs) + "')"
    else:
        query += " WHERE (%s)" % condition
        if jobs:
            query += " AND object_name IN ('" + "','".join(jobs) + "')"
    return read_query(session, query)


def get_stores(session, stores=None, condition=""):
    query = "SELECT r_object_id, name FROM dm_store"
    if is_empty(condition):
        if stores:
            query += " WHERE name IN ('" + "','".join(stores) + "')"
    else:
        query += " WHERE (%s)" % condition
        if stores:
            query += " AND name IN ('" + "','".join(stores) + "')"
    return read_query(session, query)


def read_object(session, query):
    results = read_query(session, query, 1)
    if results:
        for rec in results:
            return rec


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


def get_indexes(session):
    query = "SELECT index_name, a.object_name " \
            "FROM dm_fulltext_index i, dm_ftindex_agent_config a " \
            "WHERE i.index_name=a.index_name " \
            "AND a.force_inactive = FALSE"
    return read_query(session, query)


def get_xplore_configs(session):
    query = "SELECT ft_engine_id AS r_object_id FROM dm_fulltext_index WHERE install_loc='dsearch'"
    return read_query(session, query)


def get_acs_configs(session):
    serverconfig = session.serverconfig
    query = "SELECT r_object_id FROM dm_acs_config " \
            "WHERE svr_config_id='%s'" % serverconfig['r_object_id']
    return read_query(session, query)


class CheckSummary(Summary):
    def verbose(self, results):
        return ''

    def ok(self, results):
        return self.fmt(results)

    def problem(self, results):
        return self.fmt(results)

    def fmt(self, results):
        message = ""
        for state in [Ok, Unknown, Warn, Critical]:
            hint = ", ".join(str(x) for x in results if
                             x.state == state and not is_empty(str(x)) and (
                                 not x.metric or x.metric.context != TIME_THRESHOLDS))
            message = ", ".join(x for x in [hint, message] if not (is_empty(x)))
        return message


class CustomMetric(Metric):
    def add_message(self, message):
        self.message = message
        return self

    def replace(self, **attr):
        if hasattr(self, 'message'):
            return super(CustomMetric, self).replace(**attr).add_message(self.message)
        return super(CustomMetric, self).replace(**attr)
