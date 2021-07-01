#!/bin/bash

cp  ../development/loinc_unit_assignment/results/parameter_unit_to_loinc_mapping.csv ../data/parameter_unit_to_loinc_mapping.csv

cp  ../development/loinc_unit_assignment/results/parameter_unit_to_cleaned_unit.csv ../data/parameter_unit_to_cleaned_unit.csv

psql --host=p12.stacc.ee --dbname=egcut_epi --file=create_loinc_unit_assignment.psql 2>&1 | tee 

