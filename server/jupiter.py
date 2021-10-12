#!/usr/bin/env python -u

from __future__ import print_function
from datetime import datetime, timedelta
from subprocess import Popen
from string import Template
from time import sleep
import os.path
import json

import mysql.connector
from mysql.connector import errorcode

__author__ = 'jstoner'


def completed_agent_transactions(db, trans_start, trans_end):
    """
    :param db: database object
    :param trans_start: start date for query (string)
    :param trans_end: end date for query (string)
    :return: list of transaction IDs

    This function finds all transactions where the agent has successfully completed it actions
    """
    sql = '''
        select transid
        from log
        where
            tstamp >= %s
            and tstamp <= %s
            and stage = 'AGENT_END'
            and status = 'SUCCESS'
        '''
    trans_list = set()
    cur = db.cursor()
    cur.execute(sql, (trans_start, trans_end))
    running_transactions = cur.fetchall()
    ts_now = datetime.utcnow().isoformat(' ')
    if running_transactions is None or len(running_transactions) == 0:
        print(ts_now + ' : No Agent transactions in progress')
    else:
        for transid in running_transactions:
            print(ts_now + ' : Found Agent Transaction: ' + str(transid[0]))
            trans_list.add(transid[0])
    cur.close()
    return trans_list


def current_worker_transactions(db, trans_start, trans_end):
    """
    :param db: database object
    :param trans_start: start date for query (string)
    :param trans_end: end date for query (string)
    :return: list of transaction IDs

    This function finds all transactions where a worker has been started
    """
    sql = '''
        select transid
        from log
        where
            tstamp >= %s
            and tstamp <= %s
            and stage = 'WORKER_INIT'
            and status = 'SUCCESS'
        '''
    trans_list = set()
    cur = db.cursor()
    cur.execute(sql, (trans_start, trans_end))
    running_transactions = cur.fetchall()
    ts_now = datetime.utcnow().isoformat(' ')
    if running_transactions is None or len(running_transactions) == 0:
        print(ts_now + ' : No Worker transactions in progress')
    else:
        for transid in running_transactions:
            print(ts_now + ' : Found in-progress Worker Transaction: ' + str(transid[0]))
            trans_list.add(transid[0])
    cur.close()
    return trans_list


def completed_worker_transactions(db, trans_start, trans_end):
    """
    :param db: database object
    :param trans_start: start date for query (string)
    :param trans_end: end date for query (string)
    :return: list of transaction IDs

    This function finds all transactions where the worker has successfully completed its actions
    """
    sql = '''
        select transid
        from log
        where
            tstamp >= %s
            and tstamp <= %s
            and stage = 'WORKER_END'
            and status = 'SUCCESS'
        '''
    trans_list = set()
    cur = db.cursor()
    cur.execute(sql, (trans_start, trans_end))
    running_transactions = cur.fetchall()
    ts_now = datetime.utcnow().isoformat(' ')
    if running_transactions is None or len(running_transactions) == 0:
        print(ts_now + ' : No Worker transactions completed')
    else:
        for transid in running_transactions:
            print(ts_now + ' : Found completed Worker Transaction: ' + str(transid[0]))
            trans_list.add(transid[0])
    cur.close()
    return trans_list


def active_geos(db):
    """
    Return a list of all GEOs with agents we expect to initiate transactions
    :param db: database connection object
    :return: list of GEOs
    """
    sql = '''
        select geo_config_id
        from assignment, agent
        where
            assignment.agent_id = agent.id
            and agent.enabled = 1
            and agent.active = 1
    '''
    geo_list = set()
    cur = db.cursor()
    cur.execute(sql,)
    geos = cur.fetchall()
    if geos is None:
        return geo_list

    for geo in geos:
        geo_list.add(geo[0])
    cur.close()
    return geo_list

######
# MAIN
######
if __name__ == '__main__':
    fp = ''
    settings = ''
    loop_sleep = 120  # default
    max_time = timedelta(minutes=30)  # default
    processing = True
    cnx = False
    agent_transactions = set()
    worker_transactions = set()
    completed_transactions = set()
    available_transactions = set()

    # Load settings
    if os.path.exists('/etc/ganymede/ganymede.json'):
        fp = open('/etc/ganymede/ganymede.json', 'r')
        settings = json.load(fp)
        fp.close()
    else:
        print('Cannot find database configuration settings.')
        exit(1)

    # This dict is for connecting to Ganymede to retrieve data.
    ganymede_db_opts = {
        'user': settings['db_user'],
        'password': settings['db_pass'],
        'host': settings['db_host'],
        'database': settings['db_schema'],
        'raise_on_warnings': True,
        'time_zone': settings['db_timezone'],
    }

    # override defaults, if necessary
    if settings['jupiter_max_time']:
        if int(settings['jupiter_max_time']) > 5:
            fred = int(settings['jupiter_max_time'])
            max_time = timedelta(minutes=fred)
        else:
            print('Invalid "jupiter_max_time" setting. Using default value.')

    if settings['jupiter_loop_sleep']:
        if int(settings['jupiter_loop_sleep']) > 5:
            loop_sleep = int(settings['jupiter_loop_sleep'])
        else:
            print('Invalid "jupiter_loop_sleep" setting. Using default value.')

    # first, connect to Ganymede to gather some data
    try:
        cnx = mysql.connector.connect(**ganymede_db_opts)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print('Incorrect Ganymede DB user name or password')
            exit(1)
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print('Ganymede Schema does not exist')
            exit(1)
        else:
            print(err)
            exit(1)

    # generate some datetime strings for our query
    now = datetime.utcnow()
    start_timestamp = Template('$year-$month-$day $hour:00:00')
    end_timestamp = Template('$year-$month-$day $hour:59:59')
    start = start_timestamp.substitute(year=now.year, month=now.month, day=now.day, hour=now.hour)
    end = end_timestamp.substitute(year=now.year, month=now.month, day=now.day, hour=now.hour)

    expected_geos = active_geos(cnx)

    print(now.isoformat(' ') + ' : Jupiter initialized.')
    print(now.isoformat(' ') + ' : Expecting to process ' + str(len(expected_geos)) + ' GEOs')
    print(now.isoformat(' ') + ' : Transaction period: ' + start + ' through ' + end)

    # This goes in a loop
    while processing:

        # Termination check
        # First: see if we have exceeded our time limit
        time_check = datetime.utcnow()
        delta = time_check - now
        if delta > max_time:
            print(time_check.isoformat(' ') + ' : Exceeded max processing time.')
            processing = False
            break

        print(time_check.isoformat(' ') + ' : Scanning transaction log.')

        # we close & reopen our database connection
        cnx.close()
        cnx = mysql.connector.connect(**ganymede_db_opts)

        # make sure our sets are empty
        agent_transactions.clear()
        worker_transactions.clear()
        completed_transactions.clear()
        available_transactions.clear()

        # get all completed agent transactions
        agent_transactions = completed_agent_transactions(cnx, start, end)
        # get worker transactions that have been started
        worker_transactions = current_worker_transactions(cnx, start, end)
        # get worker transactions that have completed
        completed_transactions = completed_worker_transactions(cnx, start, end)

        print(time_check.isoformat(' ') + ' : Scan complete.')
        print(time_check.isoformat(' ') + ' : Agent Transactions: ' + str(len(agent_transactions)) +
              ', Worker Transactions In-Progress: ' + str(len(worker_transactions)) +
              ', Worker Transactions Complete: ' + str(len(completed_transactions)))

        # Next, check to see if any agents are finished
        if len(agent_transactions) == 0:
            # No agents have started, pause then loop around
            print(time_check.isoformat(' ') + ' : No Agents have completed their transactions. Pausing.')
            sleep(loop_sleep)
            continue

        # Check to see if all agent transactions have been processed by workers
        if len(expected_geos) == len(agent_transactions):
            # All expected agents have initiated transactions
            if agent_transactions == completed_transactions:
                # all agent transactions have been completed by workers, so we're done
                print(time_check.isoformat(' ') + ' : All Agent transactions have been processed.')
                processing = False
                continue

        # We have agents that have finished processing their side, figure out which have had workers launched
        # against the transaction
        available_transactions = agent_transactions - worker_transactions

        if len(available_transactions) > 0:
            # we need to launch some workers
            for x in available_transactions:
                print(time_check.isoformat(' ') + ' : Launching Worker to process transaction: ' + str(x))
                Popen(['/usr/local/bin/ganymede_worker.sh', str(x)])

        # go to sleep and loop around
        sleep(loop_sleep)

    # At this point, all processing is complete (as complete as it's going to be) so launch the Leopard
    # data set generator
    print(datetime.utcnow().isoformat(' ') + ' : Launching Extract Job')
    leopard = ['/usr/local/bin/ganymede_leopard_extract.sh']
    Popen(leopard)

    # All done
    print(datetime.utcnow().isoformat(' ') + ' : Jupiter processing complete.')
