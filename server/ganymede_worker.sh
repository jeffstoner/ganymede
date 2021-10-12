#!/bin/bash

# Ganymede Worker
#
# This script implements a worker thread for Ganymede. The functions
# it performs are:
# 1. Validate a transaction identifier
# 2. Decrypt the MySQL dump
# 3. Uncompress the MySQL dump
# 4. Create a new schema associated with the transaction
# 5. Import the dump into the schema
# 6. Call a Python script to ETL from MySQL into MongoDB
# 7. Clean up


###############################################################################
# Define variables

# TRANSID:
# This is the transaction ID. It's passed via the command line.
TRANSID="$1"

# TMPROOT:
# This is the root of a directory tree we create temporary files/dirs in
TMPROOT='/var/tmp'

# GANYMEDESETTINGS:
# The JSON-formatted settings for all scripts
GANYMEDESETTINGS='/etc/ganymede/ganymede.json'

#ISOFORMAT:
# The options for generating an ISO year-month-dayThour:minute:second
ISOFORMAT='%Y-%m-%dT%H:%M:%S'

# PYTHON:
# The Python interpreter to use
PYTHON='python'

# MYSQL:
# The MySQL client to use
MYSQL='/usr/bin/mysql'

###############################################################################
# Runtime variables. This are set during execution. Do NOT set these manually.

# DUMPDIR:
# This is the directory work work in
DUMPDIR=''

# LOGFILE:
# A log file we write stuffs to
LOGFILE=''

# SQL:
# A re-usable variable holding SQL we're executing
SQL=''

# FILENAME:
# The file name of the encrypted/compressed dump file
FILENAME=''

# KEYFILE:
# File for hold temporary key
KEYFILE=''

# NONCE:
# the key
NONCE=''

# SCHEMA:
# The schema to load the dump into
# Note - not to be confused with $GSCHEMA
SCHEMA=''

# GANYMEDE:
# The URL for contacting the Ganymede system
GANYMEDE=""

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
      # Generate a timestamp
      local NOW=$(date --utc "+${ISOFORMAT}")
      echo ${NOW} $@ >> ${LOGFILE}
      # Send the contents of the log to a responsible person
      LOG=$(cat ${LOGFILE})
      mail -s 'Ganymede: Worker' ${RCPT} <<EOM
   ERROR
   Timestamp: ${NOW}
   Message: $@

===================== LOG =========================
${LOG}
EOM
   fi
   clean_up
   exit 1
}

function abort_and_notify()
{
   # Abort the operation and notify Ganymede
   local TSTAMP=`date --utc "+${ISOFORMAT}"`
   POST /worker/log/${TRANSID} -d "timestamp=${TSTAMP}" -d "stage=${STAGE}" -d "status=FATAL" --data-urlencode "message=$@"
   abort_local "$@"
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

function log()
{
   # Logs a message
   test "$1" || return  ## no message, just return
   local NOW=`date --utc "+${ISOFORMAT}"`
   echo ${NOW} $@ >> ${LOGFILE}
}

function clean_up()
{
   # Clean up when appropriate
   test "${DUMPDIR}" || return  # no DUMPDIR exists, nothing to do
   test -d "${DUMPDIR}" || return  # DUMPDIR is not a directory, nothing to do
   test "$1" = 'SUCCESS' && { rm -rf ${DUMPDIR}; return; }
   # assume we want to keep the log file
   for sdf in ${DUMPDIR}/*
   do
      test "${sdf}" = "${LOGFILE}" && continue
      rm -f ${sdf}
   done
}

function load_dump()
{
   # Load data from a dump file passed as param 1 into a schema
   ${MYSQL} -u"${GDBUSER}" -p"${GDBPASS}" -h${GDBHOST} ${SCHEMA} < ${1} 2>${LOGFILE}
}

function create_schema()
{
   # Create a new schema
   local SCH=$(echo ${TRANSID} | tr - _)
   # Give this 3 tries
   local NUM=0
   while test $NUM -lt 3
   do
      ${MYSQL} -u"${GDBUSER}" -p"${GDBPASS}" -h${GDBHOST} -e "create schema ${SCH}" 2>${LOGFILE}
      if test $? -eq 0
      then
         # break
         NUM=4
      else
         sleep 3
         ((NUM=NUM+1))
      fi
   done
   if test ${NUM} -eq 4
   then
      echo ${SCH}
   else
      echo 'FAIL'
   fi
}

function query_ganymede()
{
   test "${SQL}" || { echo 'FAIL'; return; }
   ${MYSQL} -u"${GDBUSER}" -p"${GDBPASS}" -h${GDBHOST} ${GSCHEMA} --skip-column-names -e "${SQL}" 2>${LOGFILE}
}

function transaction_etl()
{
   test "${1}" || { echo 'FAIL'; return; }
   local SCRIPT='/usr/local/bin/ganymede_worker_etl.py'
   test -f ${SCRIPT} || { echo 'FAIL'; return; }
   local OUTPUT
   OUTPUT=$(${PYTHON} -u ${SCRIPT} --transactionid=${1})
   if test $? -eq 0
   then
      echo "${OUTPUT}"
   else
      echo 'FAIL'
   fi
}

function decrypt_file()
{
   # Decrypt a file. We expect 1 parameter, a file name for the key
   # we decrypt directly into DUMPDIR for security
   test "$1" || { echo 'FAIL'; return; }
   local NEWFILE="${DUMPDIR}/$(basename ${FILENAME} .encrypted)"
   openssl enc -aes-256-cbc -d -salt -in ${FILENAME} -out ${NEWFILE} -pass file:${1}
   if test $? -eq 0
   then
      RET=${NEWFILE}
   else
      RET='FAIL'
   fi
   # for security purposes, delete the key, regardless of outcome
   rm -f ${1}
   echo ${RET}
}

function parse_json()
{
   # Parse JSON output for a specific key
   test "$2" || return
   local KEY="$1"
   local OUT=''
   shift

   OUT=`echo "$@" | ${PYTHON} -mjson.tool | grep "${KEY}" | sed -e 's/[",]//g' | cut -d: -f2`
   echo ${OUT}
}

function  validate_environment()
{
    # This function is used to make sure things are in place before starting
    ${PYTHON} --version >/dev/null 2>/dev/null
    test $? || { abort_local "Python interpreter not found"; }
    test -x ${MYSQL} || { abort_local "MySQL interpreter not found"; }
    ${MYSQL} --version >/dev/null 2>/dev/null
    test $? || { abort_local "MySQL interpreter failed"; }

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

function resty() {
  local confdir datadir host cookies method h2t editor domain _path opt dat res ret out err verbose raw i j d tmpf args2 wantdata vimedit quote query maybe_query
  local -a curlopt
  local -a curlopt2

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
  [ "$method" = "-v" ] && echo "$_resty_host $_resty_opts" && return
  [ -z "$method" ] && echo "$_resty_host" && return
  [ -n "$_path" ] && _resty_path=$_path
  domain=$(echo -n "$_resty_host" |perl -ane '/^https?:\/\/([^\/\*]+)/; print $1')
  _path="${_resty_host//\*/$_resty_path}"

  case "$method" in
    HEAD|OPTIONS|GET|DELETE|POST|PUT|PATCH|TRACE)
      eval "curlopt2=(${_resty_opts[*]})"
      dat=$( ( [ "$wantdata" = "yes" ] && ( ( [ -n "$dat" ] && echo "$dat") || ([ ! -t 0 ] && echo "@-") ) ) || echo)
      [ -n "$dat" ] && [ "$dat" != "@-" ] && [[ $# > 0 ]] && shift
      [ "$1" = "-Z" ] && raw="yes" && [[ $# > 0 ]] && shift
      [ -n "$dat" ] && opt="--data-binary"
      [ "$method" = "HEAD" ] && opt="-I" && raw="yes"
      [ -f "$confdir/$domain" ] && eval "args2=( $(cat "$confdir/$domain" 2>/dev/null |sed 's/^ *//' |grep ^$method |cut -b $((${#method}+2))-) )"
      res=$((((curl -sLv $opt "$dat" -X $method -b "$cookies/$domain" -c "$cookies/$domain" "${args2[@]}" "${curlopt2[@]}" "${curlopt[@]}" "$_path$query" |sed 's/^/OUT /' && echo) 3>&2 2>&1 1>&3) |sed 's/^/ERR /' && echo) 2>&1)
      out=$(echo "$res" |sed '/^OUT /s/^....//p; d')
      err=$(echo "$res" |sed '/^ERR /s/^....//p; d')
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
   # Can't create log file. Don't abort but disable logging
   LOGFILE='/dev/null'
fi

STAGE='WORKER_INIT'

# Ensure sanity
validate_environment

# set up resty and get settings
GANYMEDE=$(get_config_key ${GANYMEDESETTINGS} 'api_host')
GDBHOST=$(get_config_key ${GANYMEDESETTINGS} 'db_host')
GDBUSER=$(get_config_key ${GANYMEDESETTINGS} 'db_user')
GDBPASS=$(get_config_key ${GANYMEDESETTINGS} 'db_pass')
GSCHEMA=$(get_config_key ${GANYMEDESETTINGS} 'db_schema')
UPLOADSDIR=$(get_config_key ${GANYMEDESETTINGS} 'data_dir')
resty "${GANYMEDE}/api/v1" --user-agent 'ganymede_worker/0.1' --connect-timeout 30 --max-time 180 2>/dev/null

# Validate the TRANSID
test "${TRANSID}" || { echo "Usage: $0 transaction-id"; exit 1; }
# Check the API
# From this, we assert:
# 1. the transaction ID in the log is a complete transaction if status = "SUCCESS"
# 2. the file associated with the transaction is associated with a complete transaction
# 3. the transaction is associated with a valid (and enabled) GEO
OUT=$(GET /worker/${TRANSID})
if test $? -ne 0
then
   # API call failed
   abort_and_notify "Unable to contact Ganymede: ${OUT}"
fi
if test "${OUT}"
then
   # We got a response, parse it
   # First we check for a property that only exists in a successful api call
   echo "${OUT}" | grep nonce >/dev/null 2>/dev/null
   if test $? -eq 0
   then
      NONCE=$(parse_json nonce "${OUT}")
      FILENAME=$(parse_json filename "${OUT}")
   else
      # an error message
      abort_and_notify "API returned an error: ${OUT}"
   fi
else
   # No response or mysql failed for non-obvious reasons
   abort_and_notify "No response from API or invalid transaction ID"
fi

if test "${FILENAME}"
then
   # put the correct path on it
   FILENAME="${UPLOADSDIR}/${FILENAME}"
   if test ! -s ${FILENAME}
   then
      # file does not exist, is unreadable or zero-length
      abort_and_notify "Dump file does not exist, is unreadable or is zero-length (transaction: ${TRANSID})"
   fi
else
   # No output
   abort_and_notify "No file associated with transaction ${TRANSID}"
fi

# Before we can decrypt the file, we have to get the key it was encrypted with
KEYFILE="${DUMPDIR}/nonce.out"
echo "${NONCE}" | base64 -d > ${KEYFILE}
if test -s ${KEYFILE}
then
   NONCE=''
else
   abort_and_notify "Got empty key while processing transaction: ${TRANSID}"
fi

# Stage complete
TSTAMP=`date --utc "+${ISOFORMAT}"`
POST /worker/log/${TRANSID} -d "timestamp=${TSTAMP}" -d "stage=${STAGE}" -d "status=SUCCESS" --data-urlencode "message=Initialization complete"
   
STAGE='WORKER_DECRYPT'

# Decrypt the file
OUT=$(decrypt_file ${KEYFILE})
if test "${OUT}" = 'FAIL'
then
   # Something borked
   abort_and_notify "Could not decrypt file for transaction ${TRANSID}"
else
   # the function removes the extension, so use that
   FILENAME=${OUT}
fi

# Stage complete
TSTAMP=`date --utc "+${ISOFORMAT}"`
POST /worker/log/${TRANSID} -d "timestamp=${TSTAMP}" -d "stage=${STAGE}" -d "status=SUCCESS" --data-urlencode "message=Dumpfile decrypted"

STAGE='WORKER_DECOMPRESS'

# Next step is to decompress the file
gunzip ${FILENAME}
if test $? -eq 0
then
   # Chop off the .gz extension
   FILENAME="${DUMPDIR}/$(basename ${FILENAME} .gz)"
else
   abort_and_notify "Error during file decompression (transaction ${TRANSID})"
fi

# Stage complete
TSTAMP=`date --utc "+${ISOFORMAT}"`
POST /worker/log/${TRANSID} -d "timestamp=${TSTAMP}" -d "stage=${STAGE}" -d "status=SUCCESS" --data-urlencode "message=Dump file decompressed"

STAGE='WORKER_SCHEMA'

# We have a decrypted/decompressed dump file, create a schema to load it into
OUT=$(POST /worker/${TRANSID} -d "action=CREATE")
if test "${OUT}"
then
   echo "${OUT}" | grep 'schema action completed' >/dev/null 2>/dev/null
   if test $? -eq 0
   then
      SCHEMA=G_$(echo ${TRANSID} | tr - _)
   else
      abort_and_notify "Failed to create schema for transaction ${TRANSID}"
   fi
else
   abort_and_notify "Unknown error while creating schema for transaction ${TRANSID}"
fi

# Stage complete
TSTAMP=`date --utc "+${ISOFORMAT}"`
POST /worker/log/${TRANSID} -d "timestamp=${TSTAMP}" -d "stage=${STAGE}" -d "status=SUCCESS" --data-urlencode "message=Schema created"

STAGE='WORKER_LOAD'

# With our new schema, load the data
OUT=$(load_dump ${FILENAME})
if test $? -ne 0
then
   abort_and_notify "Failed to load dump for transaction ${TRANSID}"
fi

# Stage complete
TSTAMP=`date --utc "+${ISOFORMAT}"`
POST /worker/log/${TRANSID} -d "timestamp=${TSTAMP}" -d "stage=${STAGE}" -d "status=SUCCESS" --data-urlencode "message=Transaction data loaded into MySQL"

STAGE='WORKER_ETL'

# Launch a Python script to extract the data from MySQL, transform it, then load it into MongoDB
OUT=$(transaction_etl ${TRANSID})
if test "${OUT}" = 'FAIL'
then
   abort_and_notify "Failed to ETL transaction ${TRANSID}"
fi

# Stage complete
TSTAMP=`date --utc "+${ISOFORMAT}"`
POST /worker/log/${TRANSID} -d "timestamp=${TSTAMP}" -d "stage=${STAGE}" -d "status=SUCCESS" --data-urlencode "message=Transaction ETL complete"

STAGE='WORKER_END'

# Perform clean up
# Remove the transaction data
OUT=$(POST /worker/${TRANSID} -d "action=DESTROY")
if test "${OUT}"
then
   echo "${OUT}" | grep 'schema action completed' >/dev/null 2>/dev/null
   if test $? -eq 0
   then
      # All done
      TSTAMP=`date --utc "+${ISOFORMAT}"`
      POST /worker/log/${TRANSID} -d "timestamp=${TSTAMP}" -d "stage=${STAGE}" -d "status=SUCCESS" --data-urlencode "message=Worker complete"
      clean_up SUCCESS
   else
      abort_and_notify "Failed to remove schema for transaction ${TRANSID}"
      clean_up
   fi
else
   abort_and_notify "Unknown error while removing schema for transaction ${TRANSID}"
   clean_up
fi
