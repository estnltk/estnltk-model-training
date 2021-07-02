#!/bin/bash

psql --host=p12.stacc.ee --dbname=egcut_epi --file=create_parameter_unit_to_loinc_unit_mapping.psql 2>&1 | tee 

