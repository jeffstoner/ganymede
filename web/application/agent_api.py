from __future__ import print_function
from uuid import uuid4
from datetime import datetime
import re
import os
import os.path

from base64 import b64encode
from optparse import OptionParser

import bottle
from bottle import route, request, response
import bottle_mysql
import json

api_base = '/api/v1'
data_dir = '/var/lib/ganymede/uploads'

'''Utility APIs
    Right now, it's just a "heartbeat" for monitoring purposes
'''


@route(api_base + '/heartbeat', method='GET')
def heartbeat(db):

    sql = 'select count(*) as num from agent'
    db.execute(sql)
    agent_count = db.fetchone()
    if agent_count is None:
        response.status = 503
        return

    response.set_header('X-Ganymede-agentcount', agent_count['num'])
    response.status = 200
    return agent_count


'''
Start of AGENT APIs
'''


@route(api_base + '/agent', method='GET')
def list_agents(db):
    """
        GETs a list of all agents
    """

    sql = '''
        select a.uid, a.name, a.enabled, a.active, g.short_name from agent a, geo_config g, assignment x
        where x.agent_id = a.id and x.geo_config_id = g.id order by active, enabled
        '''

    db.execute(sql)
    agents = {'agents': []}

    for row in db:
        agentapi = api_base + '/agent/' + row['uid']
        translog = agentapi + '/log'
        agent = {'agent_id': row['uid'],
                 'agent_name': row['name'], 'geo_id': row['short_name'],
                 'enabled': row['enabled'], 'active': row['active'],
                 'transaction_log_url': translog, 'agent_url': agentapi}
        agents['agents'].append(agent)

    return agents


@route(api_base + '/agent', method='POST')
def create_agent(db):
    """
      Creates a new agent
    """
    name = request.forms.get('name')
    enabled = request.forms.get('enabled')
    if name is None or enabled is None:
        msg = {'status_message': 'Invalid parameters'}
        response.status = '400 Invalid parameters'
        response.content_type = 'application/json'
        return msg

    active = 1
    if enabled == 'ENABLE':
        enabled = 1
    else:
        enabled = 0

    uid = str(uuid4())

    sql = 'insert into agent (uid, name, enabled, active) values (%s, %s, %s, %s)'
    db.execute(sql, (uid, name, enabled, active))
    if db.lastrowid:
        # db.commit()
        agenturl = api_base + '/agent/' + uid
        response.set_header('Location', agenturl)
        response.status = 201
    else:
        msg = {'status_message': 'Failed to create agent'}
        response.status = '400 Failed'
        response.content_type = 'application/json'
        return msg

    return


@route(api_base + '/agent/<agent_id:re:[a-zA-Z0-9-]+>', method='GET')
def list_agent(agent_id, db):
    """
      GETs the details of the given agent_id
    """

    # Get the basic Agent details
    sql = 'select id, uid, name, enabled, active from agent where uid = %s'
    db.execute(sql, (agent_id,))
    agent = db.fetchone()
    if agent is None:
        msg = {'status_message': 'Agent not found'}
        response.status = '404 Agent Not Found'
        response.content_type = 'application/json'
        return msg

    # Look for an assignment
    sql = '''
        select gc.short_name from geo_config gc, assignment x
        where x.agent_id = %s and gc.id = x.geo_config_id
        '''
    db.execute(sql, (agent['id'],))
    assign = db.fetchone()
    if assign is None:
        # Spit out an agent with no geo_id
        geoid = ''
    else:
        # Add the geo_id to the Agent info
        geoid = assign['short_name']

    agentapi = api_base + '/agent/' + agent['uid']
    translog = agentapi + '/log'

    agent['agent_url'] = agentapi
    agent['transaction_log_url'] = translog
    agent['agent_id'] = agent['uid']
    agent['parent_url'] = api_base + '/agent'
    agent['geo_id'] = geoid
    # delete fields that should be in the result
    del agent['uid']
    del agent['id']
    return agent


@route(api_base + '/agent/<agent_id:re:[a-zA-Z0-9-]+>', method='POST')
def update_agent(agent_id, db):
    """
      UPDATEs the given agent_id
    """

    name = request.forms.get('name')
    enabled = request.forms.get('enabled')
    if name is None and enabled is None:
        msg = {'status_message': 'Invalid parameters'}
        response.status = '400 Invalid parameters'
        response.content_type = 'application/json'
        return msg

    # make sure the agent exists
    sql = 'select id from agent where uid = %s'
    db.execute(sql, (agent_id,))
    agent = db.fetchone()
    if agent is None:
        msg = {'status_message': 'Agent not found'}
        response.status = '404 Agent Not Found'
        response.content_type = 'application/json'
        return msg

    if name is not None:
        sql = 'update agent set name = %s where uid = %s'
        db.execute(sql, (name, agent_id))

    if enabled is not None:
        if enabled == 'ENABLE':
            enabled = 1
        else:
            enabled = 0
        sql = 'update agent set enabled = %s where uid = %s'
        db.execute(sql, (enabled, agent_id))

    agenturl = api_base + '/agent/' + agent_id
    response.set_header('Location', agenturl)
    response.status = 204

    return


@route(api_base + '/agent/<agent_id:re:[a-zA-Z0-9-]+>/assign', method='POST')
def assign_agent(agent_id, db):
    """
    Assign an Agent to a GEO
    :param agent_id:
    :param db:
    :return:
    """
    geo_id = request.forms.get('geo')
    release_id = request.forms.get('release')
    if geo_id is None or release_id is None:
        msg = {'status_message': 'Invalid parameters'}
        response.status = '400 Invalid parameters'
        response.content_type = 'application/json'
        return msg

    # make sure the agent exists, get it
    sql = 'select id from agent where uid = %s'
    db.execute(sql, (agent_id,))
    agent = db.fetchone()
    if agent is None:
        msg = {'status_message': 'Agent not found'}
        response.status = '404 Agent Not Found'
        response.content_type = 'application/json'
        return msg

    # get the GEO config ID
    sql = 'select id from geo_config where short_name = %s'
    db.execute(sql, (geo_id,))
    geo = db.fetchone()
    if geo is None:
        msg = {'status_message': 'GEO not found'}
        response.status = '404 GEO Not Found'
        response.content_type = 'application/json'
        return msg

    # get the release ID
    sql = 'select id from geo_release where cc_release = %s'
    db.execute(sql, (release_id,))
    release = db.fetchone()
    if release is None:
        msg = {'status_message': 'Release not found'}
        response.status = '404 Release Not Found'
        response.content_type = 'application/json'
        return msg

    # see if the agent is already assigned to a GEO
    sql = 'select id from assignment where agent_id = %s'
    db.execute(sql, (agent['id'],))
    assignment = db.fetchone()
    if assignment is None:
        # Not assigned, so do a db INSERT to assign agent
        sql = 'insert into assignment (agent_id, geo_config_id, geo_release_id) values (%s, %s, %s)'
        rows = db.execute(sql, (agent['id'], geo['id'], release['id']))
    else:
        # Assigned, so do a db UPDATE to reassign
        sql = 'update assignment set geo_config_id = %s, geo_release_id = %s where id = %s'
        rows = db.execute(sql, (geo['id'], release['id'], assignment['id']))

    if rows > 0:
        # db.commit()
        response.status = 204
        return
    else:
        msg = {'status_message': 'Failed to assigned agent to geo'}
        response.status = '400 Failed'
        response.content_type = 'application/json'
        return msg


@route(api_base + '/agent/<agent_id:re:[a-zA-Z0-9-]+>', method='DELETE')
def delete_agent(agent_id, db):
    """
      DELETEs the given agent_id
   """

    sql = 'select id from agent where uid = %s'
    db.execute(sql, (agent_id,))
    agent = db.fetchone()
    if agent is None:
        msg = {'status_message': 'Agent not found'}
        response.status = '404 Agent Not Found'
        response.content_type = 'application/json'
        return msg

    # See if it's assigned to a GEO
    sql = 'select id from assignment where agent_id = %s'
    db.execute(sql, (agent['id'],))
    rows = db.fetchall()
    if rows is not None:
        for row in rows:
            sql = 'delete from assignment where id = %s'
            db.execute(sql, (row['id'],))

    sql = 'delete from agent where id = %s'
    db.execute(sql, (agent['id'],))
    # db.commit()
    response.status = 204
    return


@route(api_base + '/agent/<agent_id:re:[a-zA-Z0-9-]+>/log', method='GET')
def get_agent_logs(agent_id, db):
    """
      GETs a list of transactions for the given agent_id
      Query Params:
        year:
        month:
        day:
    """

    # these variables are used to build a timestamp to restrict the log query
    timestamp = ''
    timestamp_year = ''
    timestamp_month = ''
    timestamp_day = ''
    right_now = datetime.utcnow()
    tstamp_start = ''
    tstamp_end = ''

    sql = 'select a.id, x.geo_config_id from agent a, assignment x where x.agent_id = a.id and a.uid = %s'
    db.execute(sql, (agent_id,))
    agent = db.fetchone()
    if agent is None:
        msg = {'status_message': 'Agent not found'}
        response.status = '404 Agent Not Found'
        response.content_type = 'application/json'
        return msg

    parent = api_base + '/agent/' + agent_id
    log = {'transactions': [], 'parent_url': parent}

    # Check the query params, if any
    if request.query.year and int(request.query.year) >= 2015:
        timestamp_year = request.query.year

    if request.query.month and int(request.query.month) >= 1 and int(request.query.month) <= 12:
        timestamp_month = request.query.month

    if request.query.day and int(request.query.day) >= 1 and int(request.query.day) <= 31:
        timestamp_day = request.query.day

    if timestamp_year or timestamp_month or timestamp_day:
        # we have one or more part of a timestamp, so build a complete timestamp
        if timestamp_year:
            timestamp = timestamp_year
        else:
            timestamp = str(right_now.year)

        if timestamp_month:
            timestamp = timestamp + '-' + timestamp_month
        else:
            timestamp = timestamp + '-' + str(right_now.month)

        if timestamp_day:
            timestamp = timestamp + '-' + timestamp_day
        else:
            timestamp = timestamp + '-' + str(right_now.day)

    if timestamp:
        # restrict the query
        tstamp_start = timestamp + ' 00:00:00'
        tstamp_end = timestamp + ' 23:59:59'
        sql = '''
            select transid, tstamp
            from log
            where
                geo = %s and
                stage = 'AGENT_INIT' and
                tstamp >= %s and
                tstamp <= %s
            order by tstamp
        '''
        db.execute(sql, (agent['geo_config_id'], tstamp_start, tstamp_end))
    else:
        sql = '''
            select transid, tstamp from log where geo = %s and stage = 'AGENT_INIT' order by tstamp
            '''
        db.execute(sql, (agent['geo_config_id'],))

    for row in db:
        transurl = parent + '/log/' + row['transid']
        stamp = row['tstamp']
        trans = {'transaction_id': row['transid'], 'timestamp': stamp.isoformat(), 'transaction_url': transurl}
        log['transactions'].append(trans)

    return log


@route(api_base + '/agent/<agent_id:re:[a-zA-Z0-9-]+>/log', method='POST')
def create_transaction_log(agent_id, db):
    """
      Create a new transaction log
    """
    geo = request.forms.get('geo_name')
    if geo is None:
        msg = {'status_message': 'Invalid parameters'}
        response.status = '400 Invalid parameters'
        response.content_type = 'application/json'
        return msg

    timestamp = datetime.utcnow()
    sql = '''
        select x.geo_config_id, gc.short_name, gc.db_schema, gc.db_user, gc.db_pass, gc.db_host, gr.db_tables
        from assignment x, agent a, geo_release gr, geo_config gc
        where x.agent_id = a.id and x.geo_release_id = gr.id and x.geo_config_id = gc.id and a.uid = %s
        '''
    db.execute(sql, (agent_id,))
    agent = db.fetchone()
    if agent is None:
        msg = {'status_message': 'Agent not found'}
        response.status = '404 Agent Not Found'
        response.content_type = 'application/json'
        return msg

    # generate a transaction id and a nonce
    transid = str(uuid4())
    nonce = b64encode(os.urandom(256))
    sql = '''
        insert into log (transid, geo, tstamp, stage, status, message, nonce)
        values (%s, %s, %s, "AGENT_INIT", "SUCCESS", "Transaction log initiated", %s)
        '''
    db.execute(sql, (transid, agent['geo_config_id'], timestamp, nonce))
    rowid = db.lastrowid
    if rowid:
        # db.commit()
        # Return the transaction ID, db schema & credentials and list of tables to dump
        transurl = api_base + '/agent/' + agent_id + '/log/' + transid
        msg = {
            'geo_id': agent['short_name'],
            'db_host': agent['db_host'],
            'db_schema': agent['db_schema'],
            'db_user': agent['db_user'],
            'db_pass': agent['db_pass'],
            'db_tables': agent['db_tables'],
            'transaction_id': transid,
            'transaction_url': transurl,
            'nonce': nonce
        }
        response.set_header('Location', transurl)
        response.status = 201
        return msg
    else:
        msg = {'status_message': 'Failed to initiate transaction log'}
        response.status = '400 Failed'
        response.content_type = 'application/json'
        return msg


@route(api_base + '/agent/<agent_id:re:[a-zA-Z0-9-]+>/log/<logid:re:[a-zA-Z0-9-]+>', method='GET')
def get_transaction_log(agent_id, logid, db):
    """
      GET a complete transaction log
    """

    sql = 'select x.geo_config_id from assignment x, agent a where x.agent_id = a.id and a.uid = %s'
    db.execute(sql, (agent_id,))
    agent = db.fetchone()
    if agent is None:
        msg = {'status_message': 'Agent not found'}
        response.status = '404 Agent Not Found'
        response.content_type = 'application/json'
        return msg

    parent = api_base + '/agent/' + agent_id + '/log'
    log = {'transactions': [], 'parent': parent}

    sql = 'select id, tstamp, status, stage, message from log where transid = %s order by tstamp'
    db.execute(sql, (logid,))
    # noinspection PyShadowingBuiltins
    for row in db:
        stamp = row['tstamp']
        trans = {'stage': row['stage'],
                 'timestamp': stamp.isoformat(),
                 'status': row['status'],
                 'message': row['message']}
        log['transactions'].append(trans)

    return log


@route(api_base + '/agent/<agent_id:re:[a-zA-Z0-9-]+>/log/<logid:re:[a-zA-Z0-9-]+>', method='POST')
def update_transaction_log(agent_id, logid, db):
    """
      Update a transaction log with a message
    """

    # before doing db work, do some validation on the inputs
    stage = request.forms.get('stage')
    status = request.forms.get('status')
    tstamp = request.forms.get('timestamp')
    message = request.forms.get('message')

    if stage is None or status is None or tstamp is None or message is None:
        msg = {'status_message': 'Invalid parameters'}
        response.status = '403 Invalid Parameters'
        response.content_type = 'application/json'
        return msg

    valid_stage = ['AGENT_INIT', 'AGENT_DUMP', 'AGENT_COMPRESS', 'AGENT_ENCRYPT', 'AGENT_TRANSFER', 'AGENT_END']
    valid_status = ['SUCCESS', 'WARNING', 'FAIL', 'FATAL']

    if stage.upper() not in valid_stage:
        msg = {'status_message': 'Invalid stage'}
        response.status = '403 Invalid Parameters'
        response.content_type = 'application/json'
        return msg

    if status.upper() not in valid_status:
        msg = {'status_message': 'Invalid status'}
        response.status = '403 Invalid Parameters'
        response.content_type = 'application/json'
        return msg

    if re.match(r'\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d', tstamp):
        # create an actual datetime object
        timestamp = datetime.strptime(tstamp, "%Y-%m-%dT%H:%M:%S")
    else:
        # invalid date format
        msg = {'status_message': 'Invalid timestamp'}
        response.status = '403 Invalid Parameters'
        response.content_type = 'application/json'
        return msg

    if len(message) == 0:
        # no message
        msg = {'status_message': 'Invalid message'}
        response.status = '403 Invalid Parameters'
        response.content_type = 'application/json'
        return msg

    # validate the agent
    sql = 'select x.geo_config_id from assignment x, agent a where x.agent_id = a.id and a.uid = %s'
    db.execute(sql, (agent_id,))
    agent = db.fetchone()
    if agent is None:
        msg = {'status_message': 'Agent not found'}
        response.status = '404 Agent Not Found'
        response.content_type = 'application/json'
        return msg

    # make sure the transaction exists by checking for the INIT entry
    sql = '''
        select id from log where transid = %s and stage = "AGENT_INIT"
        '''
    db.execute(sql, (logid,))
    junk = db.fetchone()
    if junk is None:
        msg = {'status_message': 'Transaction log not found'}
        response.status = '404 Transaction Log Not Found'
        response.content_type = 'application/json'
        return msg

    sql = 'insert into log (transid, geo, stage, status, tstamp, message) values (%s, %s, %s, %s, %s, %s)'
    db.execute(sql, (logid, agent['geo_config_id'], stage, status, timestamp.isoformat(), message))
    rowid = db.lastrowid
    if rowid:
        # db.commit()
        response.status = 201
    else:
        msg = {'status_message': 'Failed to update transaction log'}
        response.status = '400 Failed'
        response.content_type = 'application/json'
        return msg

    return


@route(api_base + '/agent/<agent_id:re:[a-zA-Z0-9-]+>/transfer/<logid:re:[a-zA-Z0-9-]+>', method='POST')
def upload_dump(agent_id, logid, db):
    """
        POST an encrypted+compressed MySQL dump
    """

    # validate the agent
    sql = 'select x.geo_config_id from assignment x, agent a where x.agent_id = a.id and a.uid = %s'
    db.execute(sql, (agent_id,))
    agent = db.fetchone()
    if agent is None:
        msg = {'status_message': 'Agent not found'}
        response.status = '404 Agent Not Found'
        response.content_type = 'application/json'
        return msg

    # make sure the transaction exists by checking for the INIT entry
    sql = '''
        select id from log where transid = %s and stage = "AGENT_INIT"
        '''
    db.execute(sql, (logid,))
    junk = db.fetchone()
    if junk is None:
        msg = {'status_message': 'Transaction log not found'}
        response.status = '404 Transaction Log Not Found'
        response.content_type = 'application/json'
        return msg

    # make sure the transaction has not already been completed
    sql = '''
        select id from log where transid = %s and stage = "AGENT_END" and status = "SUCCESS"
        '''
    db.execute(sql, (logid,))
    junk = db.fetchone()
    if junk is not None:
        msg = {'status_message': 'Transaction closed'}
        response.status = '404 Transaction Closed'
        response.content_type = 'application/json'
        return msg

    # process the upload
    upload = request.files.get('upload')

    # split filename into a file name and file extension
    name, ext = os.path.splitext(upload.filename)
    if ext not in ('.encrypted'):
        msg = {'status_message': 'Invalid dump format'}
        response.status = '400 Invalid Dump'
        response.content_type = 'application/json'
        return msg

    try:
        upload.save(data_dir)
    except IOError as err:
        msg = {'status_message': 'Error writing dump file to storage'}
        response.status = '400 Write Error'
        response.content_type = 'application/json'
        return msg

    # write an entry to the database. Use REPLACE to support multiple upload attempts
    # Requires UNIQUE index on transid column since id column is not used in query.
    sql = 'replace into upload (transid, filename) values (%s, %s)'
    db.execute(sql, (logid, upload.filename))
    # db.commit()
    response.status = 201

    return

'''
Start of WORKER APIs
'''


@route(api_base + '/worker/<logid:re:[a-zA-Z0-9-]+>', method='GET')
def get_worker_details(logid, db):
    """
      GETs transaction details necessary for workers to do their tasks
    """

    # start by verifying that the transaction contains an AGENT_END event and is SUCCESS
    sql = '''
        select status from log where transid = %s and stage = 'AGENT_END'
        '''
    db.execute(sql, (logid,))
    log = db.fetchone()
    if log is None:
        msg = {'status_message': 'Invalid Transaction'}
        response.status = '404 Transaction Not Found'
        response.content_type = 'application/json'
        return msg

    if log['status'] != 'SUCCESS':
        msg = {'status_message': 'Transaction was not successful'}
        response.status = '404 Transaction Not Successful'
        response.content_type = 'application/json'
        return msg

    # now, pull out the data the worker requires
    sql = '''
        select u.filename, l.nonce
        from upload u, log l
        where l.transid = %s and l.stage = 'AGENT_INIT' and u.transid = l.transid
        '''
    db.execute(sql, (logid,))
    worker = db.fetchone()
    if worker is None:
        msg = {'status_message': 'Transaction not found'}
        response.status = '404 Transaction Not Found'
        response.content_type = 'application/json'
        return msg

    msg = {'filename': worker['filename'], 'nonce': worker['nonce']}
    return msg


@route(api_base + '/worker/<logid:re:[a-zA-Z0-9-]+>', method='POST')
def manage_worker_schema(logid, db):
    """
    Creates or deletes a new db schema that the worker can import data into
    :param logid:
    :return:
    """

    # change dashes into underscores
    schema = re.sub(r'-', '_', logid)
    # check what they want to do
    stage = request.forms.get('action')
    if stage.upper() == 'CREATE':
        schema_sql = 'CREATE SCHEMA IF NOT EXISTS G_{0}'.format(schema)
    elif stage.upper() == 'DESTROY':
        schema_sql = 'DROP SCHEMA IF EXISTS G_{0}'.format(schema)
    else:
        msg = {'status_message': 'Invalid Action'}
        response.status = '404 Invalid Action'
        response.content_type = 'application/json'
        return msg

    # start by verifying that the most recent transaction contains an WORKER_INIT event and is SUCCESS
    sql = '''
        select status, tstamp from log where transid = %s and stage = 'WORKER_INIT' order by tstamp desc limit 1
        '''
    db.execute(sql, (logid,))
    log = db.fetchone()
    if log is None:
        msg = {'status_message': 'Invalid Transaction'}
        response.status = '404 Transaction Not Found'
        response.content_type = 'application/json'
        return msg

    if log['status'] != 'SUCCESS':
        msg = {'status_message': 'Invalid Transaction'}
        response.status = '404 Transaction Not Found'
        response.content_type = 'application/json'
        return msg

    # now execute the action
    db.execute(schema_sql)
    # TODO: add more error checking here
    msg = {'status_message': "{0} schema action completed".format(stage.upper())}
    return msg


@route(api_base + '/worker/log/<logid:re:[a-zA-Z0-9-]+>', method='POST')
def update_worker_transaction_log(logid, db):
    """
      Update a transaction log with a message from a worker
    """

    # before doing db work, do some validation on the inputs
    stage = request.forms.get('stage')
    status = request.forms.get('status')
    tstamp = request.forms.get('timestamp')
    message = request.forms.get('message')

    if stage is None or status is None or tstamp is None or message is None:
        msg = {'status_message': 'Invalid parameters'}
        response.status = '403 Invalid Parameters'
        response.content_type = 'application/json'
        return msg

    valid_stage = ['WORKER_INIT',
                   'WORKER_DECRYPT',
                   'WORKER_DECOMPRESS',
                   'WORKER_SCHEMA',
                   'WORKER_LOAD',
                   'WORKER_ETL',
                   'WORKER_END']
    valid_status = ['SUCCESS',
                    'WARNING',
                    'FAIL',
                    'FATAL']

    if stage.upper() not in valid_stage:
        msg = {'status_message': 'Invalid stage'}
        response.status = '403 Invalid Parameters'
        response.content_type = 'application/json'
        return msg

    if status.upper() not in valid_status:
        msg = {'status_message': 'Invalid status'}
        response.status = '403 Invalid Parameters'
        response.content_type = 'application/json'
        return msg

    if re.match(r'\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d', tstamp):
        # create an actual datetime object
        timestamp = datetime.strptime(tstamp, "%Y-%m-%dT%H:%M:%S")
    else:
        # invalid date format
        msg = {'status_message': 'Invalid timestamp'}
        response.status = '403 Invalid Parameters'
        response.content_type = 'application/json'
        return msg

    if len(message) == 0:
        # no message
        msg = {'status_message': 'Invalid message'}
        response.status = '403 Invalid Parameters'
        response.content_type = 'application/json'
        return msg

    # make sure the transaction exists by checking for the INIT entry
    sql = '''
        select geo from log where transid = %s and stage = "AGENT_INIT"
        '''
    db.execute(sql, (logid,))
    geo = db.fetchone()
    if geo is None:
        msg = {'status_message': 'Transaction log not found'}
        response.status = '404 Transaction Log Not Found'
        response.content_type = 'application/json'
        return msg

    sql = 'insert into log (transid, geo, stage, status, tstamp, message) values (%s, %s, %s, %s, %s, %s)'
    db.execute(sql, (logid, geo['geo'], stage, status, timestamp.isoformat(), message))
    rowid = db.lastrowid
    if rowid:
        # db.commit()
        response.status = 201
    else:
        msg = {'status_message': 'Failed to update transaction log'}
        response.status = '400 Failed'
        response.content_type = 'application/json'
        return msg

    return


''' Utility functions
    Internal functions, not APIs
'''

'''
   MAIN
'''
if __name__ == '__main__':

    db_opts = ''
    fp = ''

    # Get options
    if os.path.exists('/etc/ganymede/ganymede.json'):
        fp = open('/etc/ganymede/ganymede.json', 'r')
        db_opts = json.load(fp)
        fp.close()
    else:
        print('Cannot find database configuration settings.')
        exit()

    parser = OptionParser()
    parser.add_option('-t', '--port', dest='port', help='The port to listen on')
    (args, unused) = parser.parse_args()

    if args.port is None:
        print('Please specify a port number')
        exit()

    pid_file = '/var/run/ganymede/agent_' + args.port + '.pid'

    with open(pid_file, 'w') as p:
        print(str(os.getpid()), file=p)

    plugin = bottle_mysql.Plugin(
        dbuser=db_opts['db_user'],
        dbpass=db_opts['db_pass'],
        dbhost=db_opts['db_host'],
        dbname=db_opts['db_schema'],
        charset=db_opts['db_charset'],
        timezone=db_opts['db_timezone'])

    bottle.install(plugin)

    bottle.run(host='127.0.0.1', port=args.port)
