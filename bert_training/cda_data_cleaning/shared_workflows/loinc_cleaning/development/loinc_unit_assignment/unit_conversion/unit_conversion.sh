#!/bin/bash

psql --host=p12.stacc.ee --dbname=egcut_epi --file=unit_conversion.psql 2>&1 | tee  unit_conversion.log

