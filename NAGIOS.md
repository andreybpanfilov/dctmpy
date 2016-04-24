Docbroker service
=================

Typical Docbroker issues are:

* Docbroker is down – somebody forgot to start it or Docbroker failed or there are connectivity issues or attacker stopped docbroker
* Content Server is not registered on Docbroker – misconfiguration on CS side or connectivity issues
* Wrong Content server is registered on Docbroker – I have seen some stupid cases when infrastructure guys clone (EMC does not provide any reliable solution for loading data into repository, so the most reliable way to perform cloning) PROD to UAT but forget to modify network settings, after that users work with wrong environment
* Attacker poisoned registration information
* Docbroker is running under DoS – for some weird reason Docbroker’s implementation is extremely ugly, and even telnet on Docbroker port causes DoS, example:
```sh
# session 1
 ~]$ nc 192.168.13.131 1489
<just enter here>
 
# session 2
~]$ time timeout 20 dmqdocbroker -c getdocbasemap
dmqdocbroker: A DocBroker Query Tool
dmqdocbroker: Documentum Client Library Version: 7.2.0000.0054
Targeting current host
Targeting port 1489
 
real    0m20.002s
user    0m0.002s
sys     0m0.000s
 ~]$ echo $?
124
```
* DoS is caused by slow client or network problems, yes, it’s weird, but client or server with network issues could affect all Documentum infrastructure, so, it is always a good idea to use different docbrokers for different services

I belive all this situations are covered by nagios_check_docbroker, some example:

Basic check of availability:
```sh
~]$ nagios_check_docbroker -H 192.168.13.131:1489
CHECKDOCBROKER OK - docbase_map_time is 6ms, Registered docbases: DCTM_DEV
| docbase_map_time=6ms;100;;0
```

The same for SSL connection (note -s flag and increased response time):

```sh
~]$ nagios_check_docbroker -H 192.168.13.131:1490 -s
CHECKDOCBROKER OK - docbase_map_time is 423ms, Registered docbases: DCTM_DEV
| docbase_map_time=423ms;;;0
```

Adding response time thresholds:

```sh
~]$ nagios_check_docbroker -H 192.168.13.131:1490 -s -w 100
CHECKDOCBROKER WARNING - docbase_map_time is 490ms (outside range 0:100), Registered docbases: DCTM_DEV
| docbase_map_time=490ms;100;;0
```

```
~]$ nagios_check_docbroker -H 192.168.13.131:1490 -s -w 100 -c 200
CHECKDOCBROKER CRITICAL - docbase_map_time is 442ms (outside range 0:200), Registered docbases: DCTM_DEV
| docbase_map_time=442ms;100;200;0
```

Checking registration of certain docbase(s) or server(s):

```sh
~]$ nagios_check_docbroker -H 192.168.13.131:1489 -d DCTM_DEV
CHECKDOCBROKER OK - docbase_map_time is 7ms, Server DCTM_DEV.DCTM_DEV is registered on 192.168.13.131:1489
| docbase_map_time=7ms;;;0
``` 

```sh
~]$ nagios_check_docbroker -H 192.168.13.131:1489 -d DCTM_DEV1
CHECKDOCBROKER CRITICAL - Docbase DCTM_DEV1 is not registered on 192.168.13.131:1489, docbase_map_time is 7ms
| docbase_map_time=7ms;;;0
```


```sh
~]$ nagios_check_docbroker -H 192.168.13.131:1489 -d DCTM_DEV1,DCTM_DEV
CHECKDOCBROKER CRITICAL - Docbase DCTM_DEV1 is not registered on 192.168.13.131:1489, docbase_map_time is 5ms, Server DCTM_DEV.DCTM_DEV is registered on 192.168.13.131:1489
| docbase_map_time=5ms;;;0
Checking registration of certain server(s):
```

```sh
~]$ nagios_check_docbroker -H 192.168.13.131:1489 -d DCTM_DEV.DCTM_DEV
CHECKDOCBROKER OK - docbase_map_time is 6ms, Server DCTM_DEV.DCTM_DEV@192.168.13.131 is registered on 192.168.13.131:1489
| docbase_map_time=6ms;;;0
```

```sh
~]$ nagios_check_docbroker -H 192.168.13.131:1489 -d DCTM_DEV.DCTM
CHECKDOCBROKER CRITICAL - Server DCTM_DEV.DCTM is not registered on 192.168.13.131:1489, docbase_map_time is 11ms
| docbase_map_time=11ms;;;0
```

```sh
~]$ nagios_check_docbroker -H 192.168.13.131:1489 -d DCTM_DEV.DCTM,DCTM_DEV.DCTM_DEV
CHECKDOCBROKER CRITICAL - Server DCTM_DEV.DCTM is not registered on 192.168.13.131:1489, docbase_map_time is 7ms, Server DCTM_DEV.DCTM_DEV@192.168.13.131 is registered on 192.168.13.131:1489
| docbase_map_time=7ms;;;0
```

Checking IP addresses of registered servers:

```sh
~]$ nagios_check_docbroker -H 192.168.13.131:1489 -d DCTM_DEV.DCTM_DEV@192.168.13.131
CHECKDOCBROKER OK - docbase_map_time is 8ms, Server DCTM_DEV.DCTM_DEV@192.168.13.131 is registered on 192.168.13.131:1489
| docbase_map_time=8ms;;;0
```

```sh
nagios_check_docbroker -H 192.168.13.131:1489 -d DCTM_DEV.DCTM_DEV@192.168.13.132
CHECKDOCBROKER CRITICAL - Server DCTM_DEV.DCTM_DEV (status: Open) is registered on 192.168.13.131:1489 with wrong ip address: 192.168.13.131, expected: 192.168.13.132, docbase_map_time is 7ms
| docbase_map_time=7ms;;;0
```

Checking malicious registrations (note -f flag):
```sh
~]$ nagios_check_docbroker -H 192.168.13.131:1489 -f -d DCTM_DEV.DCTM_DEV@192.168.13.132
CHECKDOCBROKER CRITICAL - Server DCTM_DEV.DCTM_DEV (status: Open) is registered on 192.168.13.131:1489 with wrong ip address: 192.168.13.131, expected: 192.168.13.132,  Malicious server DCTM_DEV.DCTM_DEV@192.168.13.131 (status: Open) is registered on 192.168.13.131:1489, docbase_map_time is 9ms
| docbase_map_time=9ms;;;0
```

Repository services
===================

Actually, there are a lot of things to be monitored, so nagios_check_docbase covers the most common issues. Common command line pattern for all checks is:
```sh
nagios_check_docbase -H <hostname> -p <port> -i <docbaseid> -l <username>
 -a <password> -m <mode> -n <name> [-s] [-t <timeout>] <specific arguments>
```

where:

* hostname – hostname or ip address where Documentum is running
* port – tcp port Documentum is listening on (this is not a docbroker port)
* docbaseid – docbase identifier (see docbase_id in server.ini, might be omitted but in this case you will get stupid exceptions in repository log)
* username – username to connect to Documentum
* password – password to connect to Documentum
* -s – defines whether to use SSL connection
* timeout – defines timeout in seconds after which check fails, default is 60 seconds (useful for query checks), for example:

```sh
~]$ nagios_check_docbase -H dctms://dmadmin:dmadmin@192.168.13.131:10000 \
  -m countquery \
  --query "select count(*) from dm_folder a, dm_folder b, dm_folder c"
COUNTQUERY UNKNOWN: Timeout: check execution aborted after 60s
``` 

```sh
~]$ nagios_check_docbase -H dctms://dmadmin:dmadmin@192.168.13.131:10001 \
  -m countquery -t 3600 \
  --query "select count(*) from dm_folder a, dm_folder b, dm_folder c"
COUNTQUERY OK - countquery is 14544652121
| countquery=14544652121;;;0 query_time=2703163ms;;;0
name – name of check displayed in output, default is uppercase of check name, for example:
```

```
~]$ nagios_check_docbase -H dctms://dmadmin:dmadmin@192.168.13.131:10001 -m login
LOGIN OK - user: dmadmin, connection: 1229ms, authentication: 136ms
| authentication_time=136ms;;;0 connection_time=1229ms;;;0
```

```sh
~]$ nagios_check_docbase -H dctms://dmadmin:dmadmin@192.168.13.131:10001 \
 -m login -n superuser_login
SUPERUSER_LOGIN OK - user: dmadmin, connection: 941ms, authentication: 86ms
| authentication_time=86ms;;;0 connection_time=941ms;;;0
```

mode – one of:
* sessioncount – checks count of active sessions in repository, i.e. hot_list_size in COUNT_SESSIONS RPC command result, example (last number in performance output is a value of concurrent_sessions in server.ini):

```sh
~]$ nagios_check_docbase -H dctms://dmadmin:dmadmin@192.168.13.131:10001 \
  -m sessioncount
SESSIONCOUNT OK - sessioncount is 4
| sessioncount=4;;;0;100
 
# critical threshold
~]$ nagios_check_docbase -H dctms://dmadmin:dmadmin@192.168.13.131:10001 \
   -m sessioncount -c 2
SESSIONCOUNT CRITICAL - sessioncount is 4 (outside range 0:2)
| sessioncount=4;;2;0;100
 
# warning and critical thresholds:
~]$ nagios_check_docbase -H dctms://dmadmin:dmadmin@192.168.13.131:10001 \
   -m sessioncount -w 2 -c 6
SESSIONCOUNT WARNING - sessioncount is 4 (outside range 0:2)
| sessioncount=4;2;6;0;100
```

* targets – checks whether repository is registered on all configured docbrokers, example:

```sh
~]$ nagios_check_docbase -H dctms://dmadmin:dmadmin@192.168.13.131:10001 -m targets
TARGETS OK - DCTM_DEV.DCTM_DEV has status Open on docu72dev01:1489
```

* indexagents – checks status of configured index agents, i.e. checks that status returned by FTINDEX_AGENT_ADMIN RPC is 100, example:

```sh
# no index agents configured in docbase:
~]$ nagios_check_docbase -H dctms://dmadmin:dmadmin@192.168.13.131:10001 \
   -m indexagents
INDEXAGENTS WARNING - No indexagents
 
# stopped index agent
~]$ nagios_check_docbase -H 192.168.2.56:12000/131031 -l dmadmin -a dmadmin \
     -m indexagents
INDEXAGENTS WARNING - Indexagent docu70dev01_9200_IndexAgent is stopped
```

* jobs – check job scheduling, i.e. checks whether job is in active state (might be picked up by agentexec), checks last return code of job method, checks whether agentexec honors scheduling (last check is very inaccurate because of weird agentexec implementation, so checking jobs which are supposed to be executed frequently might produce unexpected results), example:

```sh
# single job
~]$ nagios_check_docbase -H dctms://dmadmin:dmadmin@192.168.13.131:10001 -m jobs \
   --job dm_UpdateStats
JOBS OK - dm_UpdateStats last run - 1 days 02:31:34 ago
 
# multiple jobs (comma-separated list)
~]$ nagios_check_docbase -H dctms://dmadmin:dmadmin@192.168.13.131:10001 -m jobs \
   --job dm_ConsistencyChecker,dm_UpdateStats
JOBS CRITICAL - dm_ConsistencyChecker is inactive,
    dm_UpdateStats last run - 1 days 02:35:22 ago
 
# job with bad last return code
~]$ nagios_check_docbase -H dctms://dmadmin:dmadmin@192.168.13.131:10001 -m jobs \
   --job dm_usageReport
JOBS CRITICAL - dm_usageReport has status: FAILED:  
   Could not launch method dm_usageReport:  OS error: (No Error), DM error: ()
```

* nojobs – checks whether certain job is not scheduled (so it is reversed “jobs” mode) – default Documentum installation schedules certain jobs which consume a lot of resources but do nothing useful, such jobs must be disabled, example:

```sh
~]$ nagios_check_docbase -H dctms://dmadmin:dmadmin@192.168.13.131:10001 \
    -m nojobs --job dm_DBWarning
NOJOBS CRITICAL - dm_DBWarning is active
```

* timeskew – check time difference in seconds between Documentum host and monitoring server, example:

```sh
~]$ nagios_check_docbase -H dctms://dmadmin:dmadmin@192.168.13.131:10001 \
    -m timeskew
TIMESKEW OK - timeskew is 66.02
| timeskew=66.0209999084;;;0
 
# critical theshold
nagios_check_docbase -H dctms://dmadmin:dmadmin@192.168.13.131:10001 \
    -m timeskew -c 60
TIMESKEW CRITICAL - timeskew is 66.23 (outside range 0:60)
| timeskew=66.2279999256;;60;0
 
# warning and critical thesholds
nagios_check_docbase -H dctms://dmadmin:dmadmin@192.168.13.131:10001 \
    -m timeskew -w 60 -c 120
TIMESKEW WARNING - timeskew is 66.17 (outside range 0:60)
| timeskew=66.1689999104;60;120;0
```

* query – executes select statement and checks whether the count of returned rows is inside of specified threshold ranges (for checks described previously threshold ranges were trivial (i.e. “less than”), but for this check, I believe, you may want to specify more complex conditions like “count of returned rows must be greater than specified threshold”, see nagios-plugin documentation for threshold formats), additionally output might be formatted be specifying –format argument, example:

```sh
# no thresholds, just formatted output
~]$ nagios_check_docbase -H dctms://dmadmin:dmadmin@192.168.13.131:10001 \
   --query "select user_name,user_state from dm_user where user_state<>0" \
   -m query --format {user_name}:{user_state}
QUERY OK - hacker:1 - 3ms
| count=1;;;0 query_time=3ms;;;0
 
# count of rows does not exceed critical threshold 
~]$ nagios_check_docbase -H dctms://dmadmin:dmadmin@192.168.13.131:10001 \
    --query "select user_name,user_state from dm_user where user_state<>0" \
    -m query --format {user_name}:{user_state} -w 0 -c 1
QUERY WARNING - hacker:1 - 3ms (outside range 0:0)
| count=1;0;1;0 query_time=3ms;;;0
 
# count of rows is greater than or equal to critical threshold
~]$ nagios_check_docbase -H dctms://dmadmin:dmadmin@192.168.13.131:10001 \
    --query "select user_name,user_state from dm_user where user_state<>0" \
    -m query --format {user_name}:{user_state} -c 2:
QUERY CRITICAL - hacker:1 - 3ms (outside range 2:)
| count=1;;2:;0 query_time=3ms;;;0
 
# count of rows is greater than or equal to critical threshold
~]$ nagios_check_docbase -H dctms://dmadmin:dmadmin@192.168.13.131:10001 \
    --query "select user_name,user_state from dm_user where user_state<>0" \
    -m query --format {user_name}:{user_state} -c 2:
QUERY CRITICAL - hacker:1 (outside range 2:)
| count=1;;2:;0 query_time=3ms;;;0
 
# also check query execution time against thresholds
~]$ nagios_check_docbase -H dctms://dmadmin:dmadmin@192.168.13.131:10001 \
    --query "select user_name,user_state from dm_user where user_state<>0" \
    -m query --format {user_name}:{user_state} -c 2: --criticaltime 2
QUERY CRITICAL - hacker:1 - 3ms (outside range 2:)
| count=1;;2:;0 query_time=3ms;;2;0
```

* method – technically it is the same as “query” mode, but accepts only “execute do_method” queries and additionally checks value of launch_failed result attribute, I believe such approach to check health of JMS is more reliable than “jmscheck” mode (see below), example:

```
~]$ nagios_check_docbase -H dctms://dmadmin:dmadmin@192.168.13.131:10001 \
    -m method --query "execute do_method with method='JMSHealthChecker'"
METHOD OK
| query_time=14ms;;;0
```

* countquery – technically it is the same as “query” mode, but this mode assumes that query returns only single row with single attribute (actually it just picks up the first row and the first attribute in row), example:

```sh
~]$ nagios_check_docbase -H dctms://dmadmin:dmadmin@192.168.13.131:10001 \
    -m countquery --query "select count(*) from dm_sysobject"
COUNTQUERY OK - countquery is 8746
| countquery=8746;;;0 query_time=7ms;;;0
```

* workqueue – checks the total number of non-completed auto-activities for whole repository, actually it checks whether the configured number of workflow agents is sufficient or not, in some cases growth of workflow queue may indicate some issues either with workflow agent or with JMS, example:

```sh
~]$ nagios_check_docbase -H dctms://dmadmin:dmadmin@192.168.13.131:10001 \
    -m workqueue
WORKQUEUE OK - workqueue is 0
| workqueue=0;;;0
```

* serverworkqueue – checks the number of non-completed auto-activities for current server, i.e. number of auto-activities acquired by server’s workflow agent, example:

```sh
~]$ nagios_check_docbase -H dctms://dmadmin:dmadmin@192.168.13.131:10001 \
    -m serverworkqueue
SERVERWORKQUEUE OK - DCTM_DEV is 0
| DCTM_DEV=0;;;0
```

* indexqueue – checks indexagent queue size, it’s worth to combine this check with “indexagents” check, because, again, due to weird implementation of indexaget it may report “running” status, but does not process queue, example:

```
~]$ nagios_check_docbase -H 192.168.2.56:12000/131031 -l dmadmin -a dmadmin \
     -m indexqueue -w 1000 -c 2000
INDEXQUEUE CRITICAL - _fulltext_index_user is 4.978e+04 (outside range 0:2000)
| _fulltext_index_user=49781;1000;2000;0
```

* ctsqueue – the same as “indexqueue” but for CTS, no example because I do not have CTS installed
* failedtasks – checks the number of failed auto-activities, example:

```
~]$ nagios_check_docbase -H 192.168.2.56:12000/131031 -l dmadmin -a dmadmin \
       -m failedtasks
FAILEDTASKS CRITICAL - 1 task(s): 'Last Performer' (tp002-000_user1)
```

* login – checks if certain user is able to authenticate (I use this to check LDAP availability), example:

```
~]$ nagios_check_docbase -H dctms://dmadmin:dmadmin@192.168.13.131:10001 -m login
LOGIN OK - user: dmadmin, connection: 1804ms, authentication: 93ms
| authentication_time=93ms;;;0 connection_time=1804ms;;;0
 
# thresholds
~]$ nagios_check_docbase -H dctms://dmadmin:dmadmin@192.168.13.131:10001 \
    -m login --warningtime 500 --criticaltime 1000
LOGIN WARNING - user: dmadmin, connection: 909ms, authentication: 86ms
| authentication_time=86ms;500;1000;0 connection_time=909ms;500;1000;0
```

* jmsstatus – checks availability of JMS, example:

```
~]$ nagios_check_docbase -H dctms://dmadmin:dmadmin@192.168.13.131:10001 \
   -m jmsstatus
JMSSTATUS OK - http://docu72dev01:9080/DmMethods/servlet/DoMethod - 60ms, 
               http://docu72dev01:9080/DmMail/servlet/DoMail - 2ms, 
               http://docu72dev01:9080/bpm/servlet/DoMethod - 6ms
| response_time_08024be980000ced_do_bpm=6ms;;;0
response_time_08024be980000ced_do_mail=2ms;;;0
response_time_08024be980000ced_do_method=60ms;;;0
```

* ctsstatus – checks availability of CTS, no example
* acsstatus – checks availability of ACS, no example
* xplorestatus – checks availability of xPlore, no example

Because arguments host, port, docbaseid, username, password are mandatory it makes hard to create flexible setup in nagios (for example opsview allows to set only four arguments for template), so these arguments might be collapsed into the single one (host) using following convention (see previous examples):

dctm[s]://username:password@host:port/docbaseid

also password might be obfuscated using following approach:

```sh
echo -ne "password" | \
  perl -na -F// -e 'print reverse map{sprintf("%02x",(ord$_^0xB6||0xB6))}@F'
```

for example:


```sh
~]$ echo -ne dmadmin | \
> perl -na -F// -e 'print reverse map{sprintf("%02x",(ord$_^0xB6||0xB6))}@F'
d8dfdbd2d7dbd2[dmadmin@docu72dev01 ~]$
 ~]$ check_docbase.py -H dctms://dmadmin:d8dfdbd2d7dbd2@192.168.13.131:10001 \
       -m login
LOGIN OK - user: dmadmin, connection: 1805ms, authentication: 93ms
| authentication_time=93ms;;;0 connection_time=1805ms;;;0
```