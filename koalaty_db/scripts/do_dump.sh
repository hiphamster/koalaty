#!/bin/bash

# no args: {dump_location}/dump_{timestamp}.sql
# $1 == -: send to stdout

#TODO add group panda, and make /opt/* owned by that group
dump_location='/opt/panda_db/dump'

timestamp=$(date "+%m%d%y_%H%M%S")
dump_file="${dump_location}/dump_${timestamp}.sql"
dumpcmd="/usr/bin/mysqldump -h localhost -u root -ppanda --add-drop-table --complete-insert panda"


if [[ $# == 0 ]]; then
  #XXX this requires sudo (for now) 
  mkdir -p ${dump_location}
  if [[ -f $dump_file ]]; then
    rm -f $dump_file
  fi
  $dumpcmd | gzip > "${dump_file}.gz"
fi


if [[ $1 == '-' ]]; then
  $dumpcmd 2>&1
fi


