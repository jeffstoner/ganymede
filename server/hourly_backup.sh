#!/bin/bash

# hourly_backup.sh
#
# This script performs an hourly backup of the transactions that have taken place during that hour
#
###############################################################################
# Define variables

# TMPROOT:
# This is the root of a directory tree we create temporary files/dirs in
TMPROOT='/var/tmp'

# S3BUCKETTEMPLATE:
# The bucket name to use with S3 storage. NOTE: This is a format string that is passed through the 'date' command
S3BUCKETTEMPLATE='backup_%Y_%m_%d_%H'

# S3PROFILE:
# The profile name in the .s3curl file
S3PROFILE='archive'

# S3ENDPOINT:
# The URL to the S3 service
S3ENDPOINT='https://nohost.zyx'

# S3OPTS:
# Options to use when using s3curl command. These are actually curl options.
S3OPTS='--silent --insecure'

# GANYMEDESETTINGS:
# The JSON-formatted settings for all scripts
GANYMEDESETTINGS='/etc/ganymede/ganymede.json'

# GTABLES:
# The tables to dump for Ganymede
GTABLES='agent geo_config geo_release assignment log upload'

# DUMPOPTS:
# Options passed to mysqldump. Do NOT edit these unless you know what you are
# doing.
DUMPOPTS='--default-character-set=utf8 --disable-keys --extended-insert --no-create-db --quick --single-transaction --dump-date --tz-utc'

# LOGDEST:
# Where to write log messages. Values include "file" or "syslog"
# In the event a log file can't be created, this will get switched to syslog.
LOGDEST='file'

# SYSLOG_FAC:
# The syslog facility to use when LOGDEST is 'syslog'
SYSLOG_FAC='uucp'

# SYSLOG_PRI:
# The syslog priority to use when LOGDEST is 'syslog' for normal messages.
# Error messages will be logged as SYSLOG_FAC.err regardless
SYSLOG_PRI='info'

# LOGDATE:
# The timestamp format to use with logging. See man date(1) for format.
LOGDATE='%Y-%m-%d %H:%M:%S'

#ISOFORMAT:
# The options for generating an ISO year-month-dayThour:minute:second
ISOFORMAT='%Y-%m-%dT%H:%M:%S'

# S3CURL:
# The 's3curl' command
S3CURL='/usr/local/bin/s3curl'

# PYTHON
# Define the Python interpreter to use
PYTHON='python'

###############################################################################
# Runtime variables. This are set during execution. Do NOT set these manually.

# LOGFILE
# A log file somewhere in TMPROOT for writing messages
LOGFILE=''

# S3BUCKET:
# The bucket name to store stuff in
S3BUCKET=''

# MSG:
# Generic output buffer
MSG=''

# MYSQLDUMP:
# For holding the path/filename of the MySQL dump
MYSQLDUMP=''

# TSTAMPTEMPLATE:
# The format string for generating a tstamp to use in MySQL queries. This is passed through the 'date' command.
TSTAMPTEMPLATE='%Y-%m-%d %H'

# ARCHIVEFILES:
# An array of files containing the transaction dumps from the agents
declare -a ARCHIVEFILES

# INDEX:
# A generic index counter for indexing into ARCHIVEFILES
INDEX=0

# COUNT:
# A generic count variable
COUNTER=0

# ARCHIVEFAIL:
# A count of the number of archive files that failed to upload to the S3 storage platform.
ARCHIVEFAIL=0

# GDBHOST:
# The Ganymede database host
GDBHOST=''

# GDBUSER:
# The user for accessing GDBHOST
GDBUSER=''

# GDBPASS:
# The password for GDBUSER
GDBPASS=''

# GSCHEMA:
# The schema for Ganymede
GSCHEMA=''

# UPLOADSDIR:
# The directory where uploads are stored.
UPLOADSDIR=''

# ARCHIVEDIR:
# The directory to move archvies to after archiving them in S3 storage
ARCHIVEDIR=''

###############################################################################
# Functions

function abort_local()
{
   local RCPT=$(get_config_key ${GANYMEDESETTINGS} 'notify_email')
   local LOG=''

   # Abort operations. Do not notify Ganymede but obey local logging.
   if test "$1"
   then
      # Send message to log
      if test "${LOGDEST}" = 'file'
      then
         # Generate a timestamp
         NOW=$(date --utc "+${LOGDATE}")
         echo ${NOW} $@ >> ${LOGFILE}

         # Send the contents of the log to a responsible person
         LOG=$(cat ${LOGFILE})
         mail -s 'Ganymede: Hourly Backup' ${RCPT} <<EOM
   ERROR
   Timestamp: ${NOW}
   Message: $@

===================== LOG =========================
${LOG}
EOM
      elif test "${LOGDEST}" = 'syslog'
      then
         logger -p ${SYSLOG_FAC}.err -t ganymede_backup "$@"
      fi
   fi
   clean_up
   exit 1
}

function abort_and_notify()
{
   # Abort the operation and notify Ganymede
   local TSTAMP=`date --utc "+${LOGDATE}"`

   #local PARMS
   #PARMS=$(encode_parmas timestamp "${TSTAMP}" stage ${STAGE} status FATAL message "$@")
   #POST /agent/${AGENTID}/log/${TRANSID} "${PARMS}"
   abort_local "$@"
}

function log()
{
   # Logs a message
   test "$1" || return  ## no message, just return
   if test "${LOGDEST}" = 'file'
   then
      NOW=`date --utc "+${LOGDATE}"`
      echo ${NOW} $@ >> ${LOGFILE}
   elif test "${LOGDEST}" = 'syslog'
   then
      logger -p ${SYSLOG_FAC}.${SYSLOG_PRI} -t ganymede_backup "$@"
   fi
}

function clean_up()
{
   # Clean up when appropriate
   test "${DUMPDIR}" || return  # no DUMPDIR exists, nothing to do
   test -d "${DUMPDIR}" || return  # DUMPDIR is not a directory, nothing to do
   test "$1" = 'SUCCESS' && { rm -rf ${DUMPDIR}; return; }
   # Purge everything but the log file and the Ganymede dump
   for sdf in ${DUMPDIR}/*
   do
      test "${sdf}" = "${LOGFILE}" && continue
      test "${sdf}" = "${MYSQLDUMP}" && continue
      rm -f ${sdf}
   done
}

function verify_mysql()
{
   # Validate MySQL connection parameters
   local MYOUT

   MYOUT=`mysql -u"${GDBUSER}" -p"${GDBPASS}" -h"${GDBHOST}" --skip-column-names -e 'select 1' 2>/dev/null`
   if test $? -ne 0
   then
      # Some connection error
      echo 'FAIL'
   elif test $MYOUT -ne 1
   then
      # Got something weird back
      echo 'FAIL'
   else
      echo 'SUCCESS'
   fi
}

function get_config_key()
{
   # Parse JSON output for a specific key
   test "$2" || { echo ''; return; }
   local SETTINGS="$1"
   local KEY="$2"
   local OUT=''

   test -s ${SETTINGS} || { echo ''; return; }
   OUT=$(${PYTHON} -mjson.tool < ${SETTINGS} | grep "${KEY}" | sed -e 's/[",[:space:][:cntrl:]]//g' | cut -d: -f2-)
   echo ${OUT}
}

function dump_mysql()
{
   # Perform a dump of MySQL
   # Due to the nature of the Beast, we will make 3 attempts to dump MySQL, employing a back-off timing scheme.
   local NUM=0  # our current attempt
   local TRIES=3  # the number of times we will try
   local PAUSE=120  # seconds
   local TSBD=`date --utc "+ganymede_%Y-%m-%d_%H"`
   local DUMPFILE="${DUMPDIR}/${TSBD}"
   while test ${NUM} -lt ${TRIES}
   do
      test ${NUM} -eq 0 || { sleep $((NUM * PAUSE)); }
      mysqldump -u"${GDBUSER}" -p"${GDBPASS}" -h"${GDBHOST}" ${DUMPOPTS} ${GSCHEMA} ${GTABLES} > ${DUMPFILE}.sql 2>${DUMPFILE}.err
      if test $? -eq 0
      then
         # Validate the dump by checking for a specific line that's only written on success
         tail -1 ${DUMPFILE}.sql | grep '\-- Dump completed on ' >/dev/null 2>&1
         if test $? -eq 0
         then
            # Dump successful
            rm ${DUMPFILE}.err
            echo ${DUMPFILE}.sql
	    return
         fi
      fi
      NUM=$((NUM + 1))
   done
   # We failed to get a successful dump, so let's bail out
   echo 'FAIL'
}

function create_bucket()
{
   # We need a bucket name to create
   test "$1" || { echo 'FAIL'; return; }
   local BUCKET=$1
   local OUT=''

   OUT=$(${S3CURL} --id=${S3PROFILE} --createBucket -- ${S3OPTS} ${S3ENDPOINT}/${BUCKET})
   if test $? -eq 0
   then
      OUT="Created bucket ${BUCKET}"
      echo "${OUT}"
   else
      log "Failed to create bucket ${BUCKET}"
      echo 'FAIL'
   fi
}

function verify_bucket()
{
   # We need a bucket name to create
   test "$1" || { echo 'FAIL'; return; }
   local BUCKET=$1
   local OUT=''

   OUT=$(${S3CURL} --id=${S3PROFILE} --head -- ${S3OPTS} ${S3ENDPOINT}/${BUCKET} | head -1 | tr -d [:cntrl:])
   if test "${OUT}" = "HTTP/1.1 200 OK"
   then
      return 0
   else
      return 1
   fi
}

function get_archives()
{
   # Generate a list of archive files we need to backup in this run
   local COUNT=0
   local RESULT=''
   local TSTAMP=$(date --utc "+${TSTAMPTEMPLATE}")
   local START="${TSTAMP}:00:00"
   local END="${TSTAMP}:59:59"
   local SQL="select u.filename from ganymede.upload u, ganymede.log l where u.transid = l.transid and l.tstamp >= '${START}' and tstamp <= '${END}' and stage = 'AGENT_END' and status = 'SUCCESS'"

   RESULT=$(mysql -u"${GDBUSER}" -p"${GDBPASS}" -h"${GDBHOST}" --skip-column-names -e "${SQL}")
   if test $? -ne 0
   then
      # query failed
      log "Failed to query MySQL for archive files"
      return 1
   fi

   for REPLY in ${RESULT}
   do
      test "$REPLY" || continue  # skip blank lines, just in case
      log "Adding transaction file ${REPLY} to list of files to archive"
      ARCHIVEFILES[$COUNT]="$REPLY"
      ((COUNT=COUNT + 1))
   done
}

function add_archive()
{
   # we need a bucket name and a file to upload
   test $# -eq 2 || { echo 'FAIL'; return; }
   local BUCKET=$1
   local FILE=$2
   local KEY=`basename ${FILE}`  # strip any path from the filename
   local OUT=''

   if test ! -s ${FILE}
   then
      log "File (${FILE}) is not accessible"
      echo 'FAIL'
      return
   fi

   OUT=$(${S3CURL} --id=${S3PROFILE} --put=${FILE} -- ${S3OPTS} ${S3ENDPOINT}/${BUCKET}/${KEY})
   echo ${OUT} | grep 'Not Found' >/dev/null 2>/dev/null
   if test $? -eq 0
   then
      log "Failed to store archive ${FILE} to bucket ${BUCKET}"
      echo 'FAIL'
   else
      OUT="Successfully stored archive ${FILE} to bucket ${BUCKET}"
      log "${OUT}"
      echo "${OUT}"
   fi
}


###############################################################################
# MAIN

# Start by creating a temporary directory for working in
DUMPDIR=`mktemp -d ${TMPROOT}/ganymede_dump_XXXXXXXXX 2>/dev/null`
if test $? -ne 0
then
   # At this point, no log file was created, so force log to syslog
   LOGDEST='syslog'
   abort_local "Cannot create temporary directory for operations"
fi

# Create a log file
LOGFILE=`mktemp ${DUMPDIR}/log_XXXXXXXXX 2>/dev/null`
if test $? -ne 0
then
   # Can't create log file. Don't abort but force logging to syslog
   LOGDEST='syslog'
   log 'Cannot create log. Forcing logging to syslog'
fi

# Set some variables
S3BUCKET=$(date --utc "+${S3BUCKETTEMPLATE}")
GDBHOST=$(get_config_key ${GANYMEDESETTINGS} 'db_host')
GDBUSER=$(get_config_key ${GANYMEDESETTINGS} 'db_user')
GDBPASS=$(get_config_key ${GANYMEDESETTINGS} 'db_pass')
GSCHEMA=$(get_config_key ${GANYMEDESETTINGS} 'db_schema')
UPLOADSDIR=$(get_config_key ${GANYMEDESETTINGS} 'data_dir')
ARCHIVEDIR=$(get_config_key ${GANYMEDESETTINGS} 'archive_dir')

# Start by dumping the database
MSG=$(verify_mysql)
if test "${MSG}" = "SUCCESS"
then
   MSG=$(dump_mysql)
else
   abort_and_notify "MySQL not available"
fi

if test "${MSG}" = "FAIL"
then
   abort_and_notify "Failed to dump MySQL"
else
   log "Successfully dumped Ganymede database"
fi

# At this point, we have a dump file. Let's compress it.
MYSQLDUMP=${MSG}
gzip ${MYSQLDUMP} >/dev/null 2>/dev/null
if test $? -ne 0
then
   abort_and_notify "Failed to compress the dump file: ${MYSQLDUMP}"
else
   log "Successfully created Ganymede dump: ${MYSQLDUMP}.gz"
fi

# reset the filename
MYSQLDUMP="${MYSQLDUMP}.gz"

# Next, we create a new bucket
MSG=$(create_bucket ${S3BUCKET})
if test "$MSG" = 'FAIL'
then
   abort_and_notify 'Failed to create bucket'
fi

# Make sure the bucket exists
MSG=$(verify_bucket ${S3BUCKET})
if test $? -ne 0
then
   abort_and_notify "Failed to verify bucket creation"
else
   log "Verified bucket: ${S3BUCKET}"
fi

# Get our list of files
get_archives
if test $? -eq 0
then
   # Start the archive by pushing the database dump to S3 storage
   MSG=$(add_archive ${S3BUCKET} ${MYSQLDUMP})
   if test "$MSG" = 'FAIL'
   then
      # oops
      abort_and_notify "Failed to archive the Ganymede database dump"
   else
      log "Archived Ganymede dump"
      mv ${MYSQLDUMP} ${ARCHIVEDIR}/
   fi

   # Now we push the archive files themselves
   INDEX=${#ARCHIVEFILES[*]}
   COUNTER=0
   ARCHIVEFAIL=0
   while test ${COUNTER} -lt ${INDEX}
   do
      test ${ARCHIVEFILES[COUNTER]} = "" && { ((COUNTER=COUNTER + 1)); continue; }  # skip empty slots
      if test -s ${UPLOADSDIR}/${ARCHIVEFILES[COUNTER]}
      then
         MSG=$(add_archive ${S3BUCKET} ${UPLOADSDIR}/${ARCHIVEFILES[COUNTER]})
         if test "${MSG}" = 'FAIL'
         then
            # The function will log the failure for us, just count it
            ((ARCHIVEFAIL=ARCHIVEFAIL + 1))
         else
            log "Archived agent dump: ${UPLOADSDIR}/${ARCHIVEFILES[COUNTER]}"
            mv ${UPLOADSDIR}/${ARCHIVEFILES[COUNTER]} ${ARCHIVEDIR}/
         fi
      else
         # we're gonna treat this as a failure since we should not have zero-length files or files disappearing
         log "Archive file ${UPLOADSDIR}/${ARCHIVEFILES[COUNTER]} is zero-length or no longer exists"
         ((ARCHIVEFAIL=ARCHIVEFAIL + 1))
      fi
      ((COUNTER=COUNTER + 1))
   done
else
   abort_and_notify "Failed to get a list of archives"
fi

# Check our progress
if test ${ARCHIVEFAIL} -ne 0
then
   # at least one of the archives failed to transfer
   log "One or more archvies failed to transfer"
else
   log "Hourly archive operation completed successfully"
   clean_up SUCCESS
fi
