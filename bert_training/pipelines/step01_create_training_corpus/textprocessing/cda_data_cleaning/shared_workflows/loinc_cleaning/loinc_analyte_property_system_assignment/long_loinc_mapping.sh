#!/bin/bash

# Captures the idea that we get long_loinc_mapping table from CSV file managed in repository.
# In practice is suitable for development tasks, as
#   - DB name is hardcoded
#   - Target schema is hardcoded in long_loinc_mapping.psql file
#   - Creation role '*_create' is hardcoded in long_loinc_mapping.psql file
#
# In production (as part of workflow relying on configuration), we use/import CSV file via
#   analysis_data_cleaning/LOINC_cleaning/main.py
# Luigi's Task class LongLoincMappingTable.

cp ../development/loinc_analyte_property_system_assignment/results/long_loinc_mapping.csv ../data/long_loinc_mapping.csv

psql --host=p12.stacc.ee --dbname=egcut_epi --file=long_loinc_mapping.psql
