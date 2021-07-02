#!/bin/bash

if [[ $# -eq 1 ]]; then
    WORK_SCHEMA="work_$1"
else
    echo "Usage: $(basename "$0") <work_suffix>"
    exit -1
fi

echo "Going to add discharge_info to ${WORK_SCHEMA}"

psql -w -q --host=172.17.64.160 --dbname=precise4q --quiet \
     -v schema=$WORK_SCHEMA \
     --file=discharge_info.psql > /dev/null
