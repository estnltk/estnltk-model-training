#!/bin/bash

# We will call a script that does on UTHPC SLURM environment essentially:
#   module load miniconda3/4.8.2
#   conda activate medbert
#   python -m unittest discover -vvv
# and after that we will follow the output of slurm job interactively.

# Meant to be run from medbert repository root:
#   bash tests/run_quick_tests.sh
# You can do also
#   RUN_SLOW_TESTS=1 bash tests/run_quick_tests.sh
# but this will be eventually cancelled due to time limit (10min) configured in sbatch_quick_tests.sh.

# On successful job submission, SLURM prints new job identifier to the standard output.
# Like so:
#     $ sbatch tests/sbatch_quick_tests.sh
#     Submitted batch job 20351800
# We use this ID to follow test results interactively:

slurm_message=$(sbatch tests/sbatch_quick_tests.sh)

# Extract job identifier from SLURM's message.
if ! echo "${slurm_message}" | grep -q "[1-9][0-9]*$"; then
   echo "Job(s) submission failed."
   echo "${slurm_message}"
   exit 1
else
   job=$(echo "${slurm_message}" | grep -oh "[1-9][0-9]*$")
fi

# File name will be by default like following: slurm-20351800.out
# As file might not exist yet we cannot follow it with tail right away; so, we create it ourselves and follow then:
newSlurmJobLogFile="slurm-${job}.out"
touch "$newSlurmJobLogFile" ; tail -f "$newSlurmJobLogFile"
