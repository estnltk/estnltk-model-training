#!/bin/bash
#SBATCH -J mb_quick_tests
#SBATCH --partition=testing
#SBATCH --cpus-per-task=1
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --mem=500M
#SBATCH -t 00:10:00

# Submit this job so:
#   sbatch sbatch_quick_tests.sh
# and then browse the output:
#   cat slurm-{job-id}.out

echo "START: sbatch_quick_tests.sh"

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
echo "Discover and run all (non-slow) tests..."
python -m unittest discover -vvv
echo "Done, all discovered tests were run."

echo "END: sbatch_quick_tests.sh"
