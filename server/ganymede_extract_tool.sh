#!/usr/bin/env bash

# Ganymede Extraction script
#
# This script extracts data from MongoDB for blowing people's minds

# For more information, see https://wiki-engr.mcp-services.net/pages/viewpage.action?pageId=37192233

###############################################################################
# Define variables

# GANYMEDESETTINGS:
# The JSON-formatted settings for all scripts
GANYMEDESETTINGS='/etc/ganymede/ganymede.json'

# PYTHON:
# Set the Python interpreter to use
PYTHON='python'

# TMPROOT:
# This is the root of a directory tree we create temporary files/dirs in
TMPROOT="${HOME}"

# LOGDEST:
# Where to write log messages. Values include "file" or "syslog"
# In the event a log file can't be created, this will get switched to syslog.
LOGDEST='file'

# SYSLOG_FAC:
# The syslog facility to use when LOGDEST is 'syslog'
SYSLOG_FAC='local7'

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

# ARCHIVEDATE:
# The timestamp format for use as the archive file name
ARCHIVEDATE='%Y-%m-%d_%H'

# EXPORT:
# The (partial) command to perform the export of data
EXPORT='mongoexport'

# MONGOSHELL:
# The MongoDB shell program
MONGOSHELL='mongo'

# COLLECTIONS:
# The names of the MongoDB collections we work on
COLLECTIONS='geos ipblocks n1networks n2networks netdomains vendors organizations images imagedisks servers serverdisks servernics serversoftwarelabels'

# PROJECTIONS[]:
# The specific projections to use when extracting data
declare -A PROJECTIONS
PROJECTIONS['geos']='geo,report_period.year,report_period.month,report_period.day,report_period.hour,report_period.yday,report_period.epoch,ganymede_doc_version,sites.mcp_id,sites.display_name,sites.type,sites.site_name'
PROJECTIONS['ipblocks']='geo,report_period.year,report_period.month,report_period.day,report_period.hour,report_period.yday,report_period.epoch,ganymede_doc_version,general.block_type,general.subnet_size,general.state,general.in_maintenance,general.n1_network,general.n2_network'
PROJECTIONS['n1networks']='geo,report_period.year,report_period.month,report_period.day,report_period.hour,report_period.yday,report_period.epoch,ganymede_doc_version,context.context_id,context.host_name,context.context_state,context.in_maintenance,context.hypervisor_id,allocation.network_id,allocation.name,allocation.net_type,allocation.org_id,allocation.location,allocation.network_state,allocation.create_date,context.hypervisor_cluster_id'
PROJECTIONS['n2networks']='geo,report_period.year,report_period.month,report_period.day,report_period.hour,report_period.yday,report_period.epoch,ganymede_doc_version,vlan.f5_vlan_id,vlan.leaf_group_id,vlan.vlan_number,vlan.vlan_name,vlan.vlan_description,vlan.in_maintenance,allocation.cloud_network_id,allocation.org_id,allocation.datacenter_id,allocation.name,allocation.state,allocation.create_time,allocation.network_domain_id,vlan.hypervisor_id'
PROJECTIONS['netdomains']='geo,report_period.year,report_period.month,report_period.day,report_period.hour,report_period.yday,report_period.epoch,ganymede_doc_version,route_domain.route_domain_name,route_domain.in_maintenance,route_domain.hypervisor_id,allocation.network_domain_id,allocation.name,allocation.type,allocation.org_id,allocation.state,allocation.create_time'
PROJECTIONS['vendors']='geo,report_period.year,report_period.month,report_period.day,report_period.hour,report_period.yday,report_period.epoch,ganymede_doc_version,general.id,general.description,general.home_location'
PROJECTIONS['organizations']='geo,report_period.year,report_period.month,report_period.day,report_period.hour,report_period.yday,report_period.epoch,ganymede_doc_version,general.id,general.description,general.vendor,general.vendor_id,general.entity_state,general.home_location'
PROJECTIONS['images']='geo,report_period.year,report_period.month,report_period.day,report_period.hour,report_period.yday,report_period.epoch,ganymede_doc_version,general.image_id,general.org_id,general.virtual_machine_id,general.created,general.device_type,general.location,hardware.os_display_name,general.cluster_name'
PROJECTIONS['imagedisks']='general.image_id,hardware.disk.disk_type,hardware.disk.disk_id,hardware.disk.capacity.size,hardware.disk.capacity.unit,hardware.disk.speed,hardware.disk.state,report_period.year,report_period.month,report_period.day,report_period.hour,report_period.yday,report_period.epoch,ganymede_doc_version'
PROJECTIONS['servers']='geo,report_period.year,report_period.month,report_period.day,report_period.hour,report_period.yday,report_period.epoch,ganymede_doc_version,general.server_id,general.name,general.org_id,general.virtual_machine_id,general.created,general.device_type,general.location,general.state,hardware.cpu.total_cpus,hardware.cpu.cores_per_socket,hardware.cpu_speed.reservation,hardware.cpu_speed.reservation_unit,hardware.cpu_speed.limit,hardware.cpu_speed.limit_unit,hardware.ram.size,hardware.ram.unit,hardware.os_display_name,baas.asset_id,baas.service_plan,baas.state,status.started,general.cluster_name'
PROJECTIONS['serverdisks']='general.server_id,hardware.disk.disk_type,hardware.disk.disk_id,hardware.disk.unit_number,hardware.disk.capacity.size,hardware.disk.capacity.unit,hardware.disk.speed,hardware.disk.state,report_period.year,report_period.month,report_period.day,report_period.hour,report_period.yday,report_period.epoch,ganymede_doc_version'
PROJECTIONS['servernics']='general.server_id,hardware.nic.nic_id,report_period.year,report_period.month,report_period.day,report_period.hour,report_period.yday,report_period.epoch,ganymede_doc_version'
PROJECTIONS['serversoftwarelabels']='general.server_id,hardware.software_label.display_name,report_period.year,report_period.month,report_period.day,report_period.hour,report_period.yday,report_period.epoch,ganymede_doc_version'

###############################################################################
# Runtime variables. This are set during execution. Do NOT set these manually.

# DATABASE:
# The name of the database to use
DATABASE=''

# COLLECTION:
# The name of the collection in DATABASE to query
COLLECTION=''

# QUERY:
# This is the query to run for generating results
QUERY=''

# PROJECTION:
# A projection is a list of all fields in documents to be returned when the QUERY is executed
PROJECTION=''

# OUTPUT:
# This is the file to write the query results to
OUTPUT=''

# DUMPDIR:
# This is the directory work work in
DUMPDIR=''

# LOGFILE:
# A log file we write stuffs to
LOGFILE=''

# DAY:
# The day of the month to query on
DAY=''

# MONTH:
# The month to query on
MONTH=''

# YEAR:
# The year to query on
YEAR=''

# HOUR:
# The hour to query on
HOUR=''

# MONGODB:
# This is the host/IP of the MongoDB server or mongos process to connect to
MONGODB=''

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
         local NOW=$(date --utc "+${LOGDATE}")
         echo ${NOW} $@ >> ${LOGFILE}

         # Send the contents of the log to a responsible person
         LOG=$(cat ${LOGFILE})
         mail -s 'Ganymede: Leopard ETL' ${RCPT} <<EOM
   ERROR
   Timestamp: ${NOW}
   Message: $@

===================== LOG =========================
${LOG}
EOM
      elif test "${LOGDEST}" = 'syslog'
      then
         logger -p ${SYSLOG_FAC}.err -t ganymede_leopard "$@"
      fi
   fi
   clean_up
   exit 1
}

function abort_and_notify()
{
   # Abort the operation and notify Ganymede
   local TSTAMP=$(date --utc "+${ISOFORMAT}")
   abort_local "$@"
}

function log()
{
   # Logs a message
   test "$1" || return  ## no message, just return
   if test "${LOGDEST}" = 'file'
   then
      local NOW=`date --utc "+${LOGDATE}"`
      echo ${NOW} $@ >> ${LOGFILE}
   elif test "${LOGDEST}" = 'syslog'
   then
      logger -p ${SYSLOG_FAC}.${SYSLOG_PRI} -t ganymede_leopard "$@"
   fi
}

function clean_up()
{
   # Clean up when appropriate
   test "${DUMPDIR}" || return  # no DUMPDIR exists, nothing to do
   test -d "${DUMPDIR}" || return  # DUMPDIR is not a directory, nothing to do
   # Purge everything but the log file
   rm -f ${DUMPDIR}/*csv
   rm -f ${DUMPDIR}/*js
   test "$1" = 'SUCCESS' && rm -f ${LOGFILE}
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

function create_archive()
{
   # this function creates an archive
   test "${1}" || { echo 'FAIL'; return; }  # did they give us a directory?
   test -d ${1} || { echo 'FAIL'; return; }  # is it really a directory?
   test -w ${1} || { echo 'FAIL'; return; }  # can I write to it?

   local TSTAMP="${YEAR}-${MONTH}-${DAY}_${HOUR}"
   local FILE="extract_${TSTAMP}.tgz"
   local RET=''
   local HERE=$PWD
   cd ${1}
   mkdir extract
   cp *csv extract/

   tar czf ${FILE} extract/*csv
   if test $? -eq 0
   then
      RET="${1}/${FILE}"
   else
      RET='FAIL'
   fi
   rm -rf extract
   cd ${HERE}
   echo "${RET}"
}

function create_aggregation()
{
   # Create aggregations for server.disk, server.nic, server.software_label, image.disk, and mcps
   # we need to do this so we can export the data properly as CSV
   # We use the 'build_aggregations.js' as a template and substitute $QUERY for the QUERY keyword

   test -f /etc/ganymede/build_aggregations.js || abort_and_notify 'Unable to find the aggregation template'
   sed -e "s/QUERY/${QUERY}/g" -e "s/AGGID/${AGG_ID}/g" /etc/ganymede/build_aggregations.js > ${DUMPDIR}/agg.js
   sed -e "s/AGGID/${AGG_ID}/g" /etc/ganymede/clean_aggregations.js > ${DUMPDIR}/agg_clean.js

   # Run the Javascript to build the collections
   test -s ${DUMPDIR}/agg.js || abort_and_notify 'Failed to generate aggregation build script'
   ${MONGOSHELL} --quiet ${MONGODB}/${DATABASE} < ${DUMPDIR}/agg.js
}

function destroy_aggregation()
{
    # Clean up the collections we created with our aggregations

    test -f ${DUMPDIR}/agg_clean.js || abort_and_notify 'Unable to find aggregation cleanup script'
    # If the DATABASE variable is not set, don't even try
    test "${DATABASE}" || return
    ${MONGOSHELL} --quiet ${MONGODB}/${DATABASE} < ${DUMPDIR}/agg_clean.js
}


###############################################################################
# MAIN

# Some sanity checks.
# We don't like running as root. It aint necessary.
ME=$(id -u)
if test "${ME}x" = "0x"
then
    echo "Running this as root is prohibited."
    exit 1
fi

# Make sure the user's home directory exists and is writable
test -d ${TMPROOT} && test -w ${TMPROOT} || { echo "Your HOME directory is not writable. Aborting."; exit 1; }

# Start by creating a temporary directory for working in
DUMPDIR=`mktemp -d ${TMPROOT}/extract_XXXXXXXXX 2>/dev/null`
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

# set up resty and get settings
GANYMEDE=$(get_config_key ${GANYMEDESETTINGS} 'api_host')
MONGODB=$(get_config_key ${GANYMEDESETTINGS} 'mongo_host')

# Parse command line args, if any, to see what time period to extract
if test ${#} -eq 0
then
   # the easy case, use RIGHT NOW
   YEAR=$(date --utc "+%Y")
   MONTH=$(date --utc "+%-m")
   DAY=$(date --utc "+%-d")
   HOUR=$(date --utc "+%-k")
else
   while test ${#} -ne 0
   do
      OPT=${1}
      shift
      VAL=${1}
      shift
      case ${OPT}
      in
         '-y'|'-Y'|'--year'|'--YEAR')
            YEAR=${VAL}
         ;;
         '-m'|'-M'|'--month'|'--MONTH')
            MONTH=${VAL}
         ;;
         '-d'|'-D'|'--day'|'--DAY')
            DAY=${VAL}
         ;;
         '-h'|'-H'|'--hour'|'--HOUR')
            HOUR=${VAL}
         ;;
         '--help'|'--HELP')
            echo "Usage: ${0} [options]"
            echo -e "\t-y|--year YEAR"
            echo -e "\t-m|--month MONTH"
            echo -e "\t-d|--day DAY"
            echo -e "\t-h|--hour HOUR"
            echo -e "\nYou can specify zero or more parameters. Any parameter defaults to the current date or time value"
            exit
         ;;
         *)
            echo "Unknown option"
            abort_and_notify "Unknown script option"
         ;;
      esac
   done

   # Any value not provided is assumed to be NOW
   test "${YEAR}" || YEAR=$(date --utc "+%Y")
   test "${MONTH}" || MONTH=$(date --utc "+%-m")
   test "${DAY}" || DAY=$(date --utc "+%-d")
   test "${HOUR}" || HOUR=$(date --utc "+%-k")
fi

DATABASE='ganymede'
QUERY="{\"report_period.hour\" : ${HOUR}, \"report_period.year\" : ${YEAR}, \"report_period.month\" : ${MONTH}, \"report_period.day\" : ${DAY}}"
AGG_ID="${YEAR}${MONTH}${DAY}${HOUR}ex"

# Build our temporary collections using aggregations
create_aggregation

for COLLECTION in ${COLLECTIONS}
do
   PROJECTION=${PROJECTIONS[$COLLECTION]}

   case $COLLECTION
   in
      "geos")
         COLLECTION_OUT="mcps"
         COLLECTION="geos${AGG_ID}"
      ;;
      "serverdisks")
         COLLECTION_OUT="serverdisks"
         COLLECTION="serverdisks${AGG_ID}"
      ;;
      "servernics")
         COLLECTION_OUT="servernics"
         COLLECTION="servernics${AGG_ID}"
      ;;
      "serversoftwarelabels")
         COLLECTION_OUT="serversoftwarelabels"
         COLLECTION="serversoftwarelabels${AGG_ID}"
      ;;
      "imagedisks")
         COLLECTION_OUT="imagedisks"
         COLLECTION="imagedisks${AGG_ID}"
      ;;
      *)
         COLLECTION_OUT=${COLLECTION}
      ;;
   esac
   OUTPUT=$(${EXPORT} --host=${MONGODB} --db=${DATABASE} --type=csv --collection=${COLLECTION} --out=${DUMPDIR}/${COLLECTION_OUT}.csv --query="${QUERY}" --fields="${PROJECTION}" 2>&1)
   if test $? -eq 0
   then
      log "Collection: ${COLLECTION_OUT} -> ${OUTPUT}"
   else
      abort_and_notify "Error exporting collection '${COLLECTION_OUT}' from database '${DATABASE}' on host '${MONGODB}'"
   fi

done

echo `date --utc` " : Export of data for hour ${HOUR} on day ${DAY} of month ${MONTH} of year ${YEAR} complete."

# Clean up the aggregations
destroy_aggregation

# Create the archive
OUT=$(create_archive ${DUMPDIR})
if test "${OUT}" = 'FAIL'
then
   abort_and_notify "Error creating archive in ${DUMPDIR}"
fi

echo `date --utc` " : The archive is ${OUT}"
log "The archive is ${OUT}"

# All done
clean_up 'SUCCESS'
