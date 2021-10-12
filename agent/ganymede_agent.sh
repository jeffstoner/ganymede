#!/bin/bash

# Ganymede Agent
# v 0.1
#
# This Agent is responsible for the following actions:
# 1. Contacting the Ganymede server to initiate a transaction
# 2. Dumping the specified tables from the specified schema
# 3. Compressing the dump
# 4. Encrypting the dump
# 5. Transferring the dump to the Ganymede server
# 6. Contacting the Ganymede server to end the transaction
#

###############################################################################
# Define variables

# AGENTID:
# This is the agent's unique identifier
AGENTID=''

# AGENTHOME:
# This is the Agent's "home" directory
AGENTHOME='/home/tomcat'

# GANYMEDE:
# The URL for contacting the Ganymede system
GANYMEDE="http://x.x.x.x"

# TMPROOT:
# A location for holding a temporary working directory of files.
TMPROOT="/tmp"

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

# DUMPOPTS:
# Options passed to mysqldump. Do NOT edit these unless you know what you are
# doing.
DUMPOPTS='--default-character-set=utf8 --disable-keys --extended-insert --no-create-db --quick --single-transaction --dump-date --tz-utc'

# PYTHON:
# The Python interpreter to use. Note: For RHEL 5, set to 'python26'
PYTHON='python2'

# MYSQL:
# The MySQL client to use
MYSQL='mysql'

# MYSQLDUMP:
# The MySQL Dump program to use
MYSQLDUMP='mysqldump'

###############################################################################
# Runtime variables. This are set during execution. Do NOT set these manually.

# TRANSID:
# The Transaction ID we weill be working in.
TRANSID=''

# TOKEN:
# A token to use with encryption.
TOKEN=''

# LOGFILE
# A log file somewhere in TMPROOT for writing messages
LOGFILE=''

# STAGE:
# The stage of operations (INIT, DUMP, COMPRESS, ENCRYPT, TRANSFER)
STAGE=''

# STATUS:
# The status of the current operation. (SUCCESS, WARNING, FAIL, FATAL)
STATUS=''

# DBSCHEMA:
# The database schema to dump
DBSCHEMA=''

# DBHOST:
# The database host to connect to.
DBHOST=''

# DBUSER:
# The user to connect to the database.
DBUSER=''

# DBPASS:
# The password for DBUSER
DBPASS=''

# DBTABLES:
# A List of tables to dump from the DBSCHEMA schema
DBTABLES=''

# TRANSID:
# The Ganymede transaction ID for reporting purposes
TRANSID=''

# NONCE:
# A randomly-generated nonce used for encryption/decryption
NONCE=''

# PARMS:
# URL-encoded parameters for making API calls
PARMS=''

# ENABLED:
# A boolean indicating whether the Agent is enabled or disabled
ENABLED=''
###############################################################################
# Functions
function abort_local()
{
   # Abort operations. Do not notify Ganymede but obey local logging.
   if test "$1"
   then
      # Send message to log
      if test "${LOGDEST}" = 'file'
      then
         # Generate a timestamp
         NOW=`date --utc "+${LOGDATE}"`
         echo ${NOW} $@ >> ${LOGFILE}
      elif test "${LOGDEST}" = 'syslog'
      then
         logger -p ${SYSLOG_FAC}.err -t ganymede_agent "$@"
      fi
   fi
   clean_up
   exit 1
}

function abort_and_notify()
{
   # Abort the operation and notify Ganymede
   TSTAMP=`date --utc "+${ISOFORMAT}"`
   local PARMS
   PARMS=$(encode_params timestamp "${TSTAMP}" stage ${STAGE} status FATAL message "$@")
   POST /agent/${AGENTID}/log/${TRANSID} "${PARMS}"
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
      logger -p ${SYSLOG_FAC}.${SYSLOG_PRI} -t ganymede_agent "$@"
   fi
}

function clean_up()
{
   # Clean up when appropriate
   test "${DUMPDIR}" || return  # no DUMPDIR exists, nothing to do
   test -d "${DUMPDIR}" || return  # DUMPDIR is not a directory, nothing to do
   test "$1" = 'SUCCESS' && { rm -rf ${DUMPDIR}; return; }
   if test "${LOGDEST}" = 'file'
   then
      # Purge everything but the log file
      for sdf in ${DUMPDIR}/*
      do
         test "${sdf}" = "${LOGFILE}" && continue
         rm -f ${sdf}
      done
   else
      # All logging was to syslog, so just axe the whole directory
      rm -rf ${DUMPDIR}
   fi
}

function  validate_environment()
{
    # This function is used to make sure things are in place before starting
    ${PYTHON} --version >/dev/null 2>/dev/null
    test $? || { abort_local "Python interpreter not found"; }
    #test -x ${MYSQL} || { abort_local "MySQL interpreter not found"; }
    ${MYSQL} --version >/dev/null 2>/dev/null
    test $? || { abort_local "MySQL interpreter failed"; }
    #test -x ${MYSQLDUMP} || { abort_local "MySQL Dump program not found"; }
    ${MYSQLDUMP} --version >/dev/null 2>/dev/null
    test $? || { abort_local "MySQL Dump failed"; }

    return 0
}

#
# resty - A tiny command line REST interface for bash and zsh.
#
# Fork me on github:
#   http://github.com/micha/resty
#
# Author:
#   Micha Niskin <micha@thinkminimo.com>
#   Copyright 2009, no rights reserved.
#

export _resty_host=""
export _resty_path=""
export _resty_nohistory="/dev/null"

function resty()
{
  local confdir datadir host cookies method h2t editor domain _path opt dat res ret out err verbose raw i j d tmpf args2 wantdata vimedit quote query maybe_query
  local -a curlopt
  local -a curlopt2
  local resty_out resty_err

  confdir="${DUMPDIR}/.resty"
  datadir="$confdir"
  mkdir -p "$confdir"
  host="$datadir/host"
  cookies="$datadir/c"
  method="$1"; [[ $# > 0 ]] && shift

  [ "${method#P}" != "$method" ] || [ "$method" = "TRACE" ] && wantdata="yes"

  [ -d "$cookies" ] || (mkdir -p "$cookies"; echo "http://localhost*" > "$host")
  [ -n "$1" ] && [ "${1#/}" != "$1" ] && _path="$1" && [[ $# > 0 ]] && shift
  [ "$1" = "${1#-}" ] && dat="$1" && [[ $# > 0 ]] && shift
  
  j=1
  for i in "$@"; do
    [ -n "$maybe_query" -a -z "$query" ] && query="?$i" && continue
    ([ "$i" = "--verbose" ] || echo "$i" | grep '^-[a-zA-Z]*v[a-zA-Z]*$' >/dev/null) \
      && verbose="yes" && continue
    [ "$i" = "-Z" ] && raw="yes" && continue
    [ "$i" = "-W" ] && continue
    [ "$i" = "-Q" ] && quote="yes" && continue
    [ "$i" = "-q" ] && maybe_query="yes" && continue
    curlopt[j]="$i" && j=$((j + 1))
  done

  [ -z "$quote" ] && _path=$(echo "$_path"|sed 's/%/%25/g;s/\[/%5B/g;s/\]/%5D/g;s/|/%7C/g;s/\$/%24/g;s/&/%26/g;s/+/%2B/g;s/,/%2C/g;s/:/%3A/g;s/;/%3B/g;s/=/%3D/g;s/?/%3F/g;s/@/%40/g;s/ /%20/g;s/#/%23/g;s/{/%7B/g;s/}/%7D/g;s/\\/%5C/g;s/\^/%5E/g;s/~/%7E/g;s/`/%60/g')

  [ "$method" = "HEAD" ] || [ "$method" = "OPTIONS" ] && raw="yes"
  [ -z "$_resty_host" ] && _resty_host=$(cat "$host" 2>/dev/null)
  [ -z "$method" ] && echo "$_resty_host" && return
  [ -n "$_path" ] && _resty_path=$_path
  domain=$(echo -n "$_resty_host" |perl -ane '/^https?:\/\/([^\/\*]+)/; print $1')
  _path="${_resty_host//\*/$_resty_path}"

  case "$method" in
    HEAD|OPTIONS|GET|DELETE|POST|PUT|PATCH|TRACE)
      eval "curlopt2=(${_resty_opts[*]})"
      dat=$( ( [ "$wantdata" = "yes" ] && ( [ -n "$dat" ] && echo "$dat" ) ) || echo)
      [ -n "$dat" ] && [ "$dat" != "@-" ] && [[ $# > 0 ]] && shift
      [ "$1" = "-Z" ] && raw="yes" && [[ $# > 0 ]] && shift
      [ -n "$dat" ] && opt="--data-binary"
      [ "$method" = "HEAD" ] && opt="-I" && raw="yes"
      [ -f "$confdir/$domain" ] && eval "args2=( $(cat "$confdir/$domain" 2>/dev/null |sed 's/^ *//' |grep ^$method |cut -b $((${#method}+2))-) )"
      # Dump stdout to resty_out and stderr to resty_err
      resty_out=$(mktemp ${datadir}/.resty_stdout_XXXXXXXX)
      resty_err=$(mktemp ${datadir}/.resty_stderr_XXXXXXXX)
      res=$(curl -sLv $opt "$dat" -X $method \
              -b "$cookies/$domain" -c "$cookies/$domain" \
              "${args2[@]}" "${curlopt2[@]}" "${curlopt[@]}" "$_path$query" >${resty_out} 2>${resty_err})
      out=$(cat ${resty_out})
      err=$(cat ${resty_err})
      rm ${resty_out} ${resty_err}
      ret=$(echo "$err" |sed  '/^.*HTTP\/1\.[01] [0-9][0-9][0-9]/s/.*\([0-9]\)[0-9][0-9].*/\1/p; d' | tail -n1)
      [ -n "$err" -a -n "$verbose" ] && echo "$err" 1>&2
      echo "$err" | grep -i '^< \s*Content-Type:  *text/html' >/dev/null \
        && [ -z "$raw" ] && d=$h2t || d=cat
      [ -n "$out" ] && out=$(echo "$out" |eval "$d")
      [ "$d" != "${d##lynx}" ] && out=$(echo "$out" |perl -e "\$host='$(echo "$_resty_host" |sed 's/^\(https*:\/\/[^\/*]*\).*$/\1/')';" -e '@a=<>; $s=0; foreach (reverse(@a)) { if ($_ =~ /^References$/) { $s++; } unless ($s>0) { s/^\s+[0-9]+\. //; s/^file:\/\/localhost/$host/; } push(@ret,$_); } print(join("",reverse(@ret)))')
      if [ "$ret" != "2" ]; then
        [ -n "$out" ] && echo "$out" 1>&2
        return $ret
      else
        [ -n "$out" ] && echo "$out"
      fi
      ;;
    http://*|https://*)
      _resty_opts=$(printf '%q ' "${curlopt[@]}")
      export _resty_opts
      echo "$method" |grep '\*' >/dev/null || method="${method}*"
      (echo "$method" |tee "${_resty_nohistory:-$host}") |cat 1>&2 \
        && _resty_host="$method"
      ;;
    *)
      resty "http://$method" "${curlopt[@]}"
      ;;
  esac
}

function HEAD() {
  resty HEAD "$@"
}

function OPTIONS() {
  resty OPTIONS "$@"
}

function GET() {
  resty GET "$@"
}

function POST() {
  resty POST "$@"
}

function PUT() {
  resty PUT "$@"
}

function PATCH() {
  resty PATCH "$@"
}

function DELETE() {
  resty DELETE "$@"
}

function TRACE() {
  resty TRACE "$@"
}

function parse_json()
{
   # Parse JSON output for a specific key
   test "$2" || return
   local KEY="$1"
   local OUT=''
   shift

   OUT=`echo "$@" | ${PYTHON} -mjson.tool | grep "${KEY}" | sed -e 's/[",]//g' | cut -d: -f2-`
   echo ${OUT}
}

function verify_mysql()
{
   # Validate MySQL connection parameters
   local MYOUT

   MYOUT=`${MYSQL} -u"${DBUSER}" -p"${DBPASS}" -h"${DBHOST}" --skip-column-names -e 'select 1' 2>/dev/null`
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

function dump_mysql()
{
   # Perform a dump of MySQL
   # Due to the nature of the Beast, we will make 3 attempts to dump MySQL, employing a back-off timing scheme.
   local NUM=0  # our current attempt
   local TRIES=3  # the number of times we will try
   local PAUSE=120  # seconds
   local DUMPFILE="${DUMPDIR}/sql_${GEO}_${TRANSID}"
   while test ${NUM} -lt ${TRIES}
   do
      test ${NUM} -eq 0 || { sleep $((NUM * PAUSE)); }
      ${MYSQLDUMP} -u"${DBUSER}" -p"${DBPASS}" -h"${DBHOST}" ${DUMPOPTS} ${DBSCHEMA} ${DBTABLES} > ${DUMPFILE}.sql 2>${DUMPFILE}.err
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

function encrypt_dump()
{
   # Encrypt the dump file
   # Parameters: $1 - the file to be encrypted, $2 - the file containing the passphrase
   test "$2" || { echo 'FAIL'; return; }
   local RET=''
   openssl enc -aes-256-cbc -salt -in ${1} -out ${1}.encrypted -pass file:${2}
   if test $? -eq 0
   then
      RET="${1}.encrypted"
   else
      RET='FAIL'
   fi
   # for security, delete the key regardless of outcome
   rm -f ${2}
   echo ${RET}
}

function encode_params()
{
   local KEY
   local VAL
   local PYSTRING
   local OUT
   local NUM

   # Do some sanity checking
   test $# -eq 0 && return  ## no arguments
   NUM=$(expr $# % 2)
   test ${NUM} -eq 1 && return  ## an odd-number of arguments

   while test $# -ne 0
   do
      KEY=$1
      VAL=$2
      shift 2
      if test "$PYSTRING" = ""
      then
         PYSTRING="'${KEY}' : '${VAL}'"
      else
         PYSTRING="${PYSTRING},'${KEY}' : '${VAL}'"
      fi
   done
   OUT=$(${PYTHON} -c "
from __future__ import print_function
import urllib
print(urllib.urlencode({${PYSTRING}}))")

   echo $OUT
}

###############################################################################
# MAIN

# Start by creating a temporary directory for working in
DUMPDIR=`mktemp -d ${TMPROOT}/ganymede_XXXXXXXXX 2>/dev/null`
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

validate_environment

# We will accept an agent name on the command line
if test "$1"
then
   AGENTID=$1
fi

test "${AGENTID}" || abort_local "AGENTID not set"

# So that all agents do not slam Ganymede at the same time, pause for up to 10 minutes
sleep $((RANDOM % 600))

###############################################################################
# Stage: INIT

STAGE='AGENT_INIT'

# set up resty
resty "${GANYMEDE}/api/v1" --user-agent 'ganymede_agent/0.1' --connect-timeout 30 --max-time 180 2>/dev/null

OUT=`GET /agent/${AGENTID}`
ENABLED=$(parse_json enabled $OUT)
GEO=$(parse_json geo_id $OUT)

if test "$GEO"
then
   # Initialize a transaction
   OUT=`POST /agent/${AGENTID}/log "geo_name=${GEO}"`
else
   abort_local "Could not get identity from Ganymede"
fi

if test "$OUT"
then
   # Parse output to grab db_schema, db_user, db_post, db_tables and transaction_id
   DBSCHEMA=$(parse_json db_schema $OUT)
   DBHOST=$(parse_json db_host $OUT)
   DBUSER=$(parse_json db_user $OUT)
   DBPASS=$(parse_json db_pass $OUT)
   DBTABLES=$(parse_json db_tables $OUT)
   TRANSID=$(parse_json transaction_id $OUT)
   NONCE=$(parse_json nonce $OUT)
else
   # hrmm
   abort_local "Unable to initiate transaction"
fi

if test "${TRANSID}"
then
   log "Agent:${AGENTID} GEO:${GEO} Transaction:${TRANSID} Host:${DBHOST}"
else
   abort_local "Unable to identify Transaction Id"
fi

if test "$ENABLED" = "0"
then
   # We've been disabled. Don't do anything else
   abort_and_notify "Agent is disabled"
fi

###############################################################################
# Stage: DUMP

STAGE='AGENT_DUMP'

if test $(verify_mysql) = "SUCCESS"
then
   # dump the database
   OUT=$(dump_mysql)
else
   abort_and_notify "Cannot access database server"
fi

# Check the results of the dump
if test "${OUT}" = "FAIL"
then
   abort_and_notify "Failed to dump database"
else
   log "MySQL dump complete"
fi

# Send notification to Ganymede
TSTAMP=`date --utc "+${ISOFORMAT}"`
PARMS=$(encode_params timestamp "${TSTAMP}" stage ${STAGE} status SUCCESS message 'MySQL dump complete')
POST /agent/${AGENTID}/log/${TRANSID} "${PARMS}"


###############################################################################
# Stage: COMPRESS

STAGE='AGENT_COMPRESS'

PRECOMPRESS=$(ls -l ${OUT} | awk '{print $5}')
gzip ${OUT}
if test $? -ne 0
then
   # Some processing error during compression
   abort_and_notify "Compression error"
fi

DUMPFILE="${OUT}.gz"
POSTCOMPRESS=$(ls -l ${DUMPFILE} | awk '{print $5}')

log "Compression complete. Original size: ${PRECOMPRESS}, Compressed size: ${POSTCOMPRESS}"

# Send notification to Ganymede
TSTAMP=`date --utc "+${ISOFORMAT}"`
PARMS=$(encode_params timestamp "${TSTAMP}" stage ${STAGE} status SUCCESS message "Uncompressed size: ${PRECOMPRESS}, Compressed size: ${POSTCOMPRESS}")
POST /agent/${AGENTID}/log/${TRANSID} "${PARMS}"


###############################################################################
# Stage: ENCRYPT

STAGE='AGENT_ENCRYPT'

KEYFILE="${DUMPDIR}/nonce.out"
# Start by base64 decoding the nonce and write it to a file
echo -n "${NONCE}" | base64 -d - > ${KEYFILE}
OUT=$(encrypt_dump ${DUMPFILE} ${KEYFILE})
if test "${OUT}" = 'FAIL'
then
   abort_and_notify "Encryption error"
else
   # remove the dump
   rm -f ${DUMPFILE}
   FILESIZE=$(ls -l ${OUT} | awk '{print $5}')
   log "Dump file successfully encrypted. Size: ${FILESIZE}"
fi

# Send notification to Ganymede
TSTAMP=`date --utc "+${ISOFORMAT}"`
PARMS=$(encode_params timestamp "${TSTAMP}" stage ${STAGE} status SUCCESS message "Dump encrypted and ready for transfer. Size: ${FILESIZE}")
POST /agent/${AGENTID}/log/${TRANSID} "${PARMS}"

###############################################################################
# Stage: TRANSFER

STAGE='AGENT_TRANSFER'

FILENAME=$(basename ${OUT})
# retry the transfer up to 4 times
TRANS_COUNT=1
MAX_RETRANS=6
while test ${TRANS_COUNT} -lt ${MAX_RETRANS}
do
   HTTPOUT=$(POST /agent/${AGENTID}/transfer/${TRANSID} -F "upload=@${OUT};filename=${FILENAME}" 2>&1)
   if test -n "${HTTPOUT}"
   then
      log "Attempt ${TRANS_COUNT} to transfer encrypted dump failed: ${HTTPOUT}"
      ((TRANS_COUNT = TRANS_COUNT + 1))
      # pause for a few seconds
      sleep 5
   else
      # successful transfer. set TRANS_COUNT to 10 (a value it could never reach unless this code is run
      log "Successfully transferred encrypted dump file"
      TRANS_COUNT=10
   fi
done

if test ${TRANS_COUNT} -eq 10
then
   log 'File transfer complete'
   # Send notification to Ganymede
   TSTAMP=`date --utc "+${ISOFORMAT}"`
   PARMS=$(encode_params timestamp "${TSTAMP}" stage ${STAGE} status SUCCESS message "Transfer complete")
   POST /agent/${AGENTID}/log/${TRANSID} "${PARMS}"
else
   abort_and_notify "All attempts to transfer encrypted dump file failed. Aborting."
fi
###############################################################################
# Stage: END

STAGE='AGENT_END'

# Just tell Ganymede that we are done
TSTAMP=`date --utc "+${ISOFORMAT}"`
PARMS=$(encode_params timestamp "${TSTAMP}" stage ${STAGE} status SUCCESS message "Processing complete")
POST /agent/${AGENTID}/log/${TRANSID} "${PARMS}"
clean_up SUCCESS

exit 0
