#!/bin/bash

# Do not use hard-coded path to the repo root/checkout directory/code root
### export PYTHONPATH=~/Repos/cda-data-cleaning:$PYTHONPATH
# Instead, calculate code root from current bash file, and not depending from where it is executed, i.e.
# PYTHONPATH will be /full/path-to/cda-data-cleaning where cda-data-cleaning/ is the root of the repo/checkout
REPO_ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/../../.." >/dev/null 2>&1 && pwd )"
export PYTHONPATH=$REPO_ROOT_DIR
echo "Repository root directory is $REPO_ROOT_DIR which we use as PYTHONPATH".

# Default values for optional parameters:

# NB! Do not edit *.example.ini files -- instead create your own (all but specific example files are
#     rejected via .gitignore as this way there is a smaller chance that you reveal secret bits of your config)
conf="$REPO_ROOT_DIR/configurations/egcut_epi_microrun.app_extractor.ini"
workers=20

# The default is empty so that cda_job will take work_schema from config
work_schema=""
prefix=""

# Reading in parameters
for i in "$@"
do
case $i in
    -c=*|--conf=*)
    conf="${i#*=}"
    ;;
    -s=*|--work-schema=*)
    work_schema="${i#*=}"
    ;;
    -p=*|--prefix=*)
    prefix="${i#*=}"
    ;;
    -w=*|--workers=*)
    workers="${i#*=}"
    ;;
    *)
            # unknown option
    ;;
esac
done


# Instructions to execute whole cleaning pipeline for 'original.analysis' table.
# Instructions by Anne, from https://redmine.stacc.ee/redmine/issues/1208#note-19
#
# NB! One needs to use 'luigid --port 8082' (i.e. central scheduler) or local scheduler (via flag: '--local-scheduler')
# For central scheduler, start in separate session/terminal:
#     luigid --port 8082
luigi --scheduler-port 8082 \
      --module cda_data_cleaning.data_cleaning.analysis_data_cleaning.run_analysis_data_cleaning RunAnalysisDataCleaning \
      --prefix="${prefix}" \
      --config=${conf} \
      --work-schema=${work_schema} \
      --workers=${workers} \
      --log-level=INFO
