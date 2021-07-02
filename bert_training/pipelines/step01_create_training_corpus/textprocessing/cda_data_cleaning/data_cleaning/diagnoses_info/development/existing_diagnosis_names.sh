#!/bin/bash

if [[ $# -eq 1 ]]; then
    DB_NAME="$1"
else
    echo "Usage: $(basename "$0") <db_name>"
    exit -1
fi

echo "Extracting existing diagnoses code names from database ${DB_NAME}"

psql -w -q --host=p12.stacc.ee --dbname=${DB_NAME} --quiet \
     -v schema=orignal \
     --file=existing_diagnosis_names.psql > /dev/null
