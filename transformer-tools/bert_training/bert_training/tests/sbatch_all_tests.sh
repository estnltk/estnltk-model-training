#!/bin/bash
#SBATCH -J mb_all_tests
#SBATCH --partition=main
#SBATCH --cpus-per-task=4
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --mem=8G
#SBATCH -t 03:00:00

# Note that testing partition has time limit 120 minutes:
#   sbatch: error: You have requested too much time or specified no time limit for your job to run on a partition.
#                  Maximum for partition 'testing' is 120 minutes and you requested 180 minutes
#   sbatch: error: Batch job submission failed: Requested time limit is invalid (missing or exceeds some limit)

# Submit this job so:
#   sbatch sbatch_all_tests.sh
# and then browse the output:
#   cat slurm-{job-id}.out

echo "START: sbatch_all_tests.sh"

echo "Load module 'miniconda3/4.8.2'..."
module load miniconda3/4.8.2
echo "Module loading done."

# MAGIC to avoid
#   CommandNotFoundError: Your shell has not been properly configured to use 'conda activate'.
echo "Prepare for conda env activation ..."
echo $'Calling: eval "$(conda shell.bash hook)"'
eval "$(conda shell.bash hook)"
echo "Done."

echo "Activate conda environment 'medbert'..."
conda activate medbert
echo "Done, env activated."

# Slow tests are skipped by default, others can be run like so:
# (medbert) $ python -m unittest discover -vvv
echo "Discover and run all tests (including slow ones)..."
RUN_SLOW_TESTS=1 python -m unittest discover -vvv
echo "Done, all discovered tests were run."

echo "END: sbatch_all_tests.sh"
