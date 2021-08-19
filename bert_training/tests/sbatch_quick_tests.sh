#!/bin/bash
#SBATCH -J mb_quick_tests
#SBATCH --partition=testing
#SBATCH --cpus-per-task=1
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --mem=500M
#SBATCH -t 00:10:00

module load miniconda3/4.8.2

# MAGIC to avoid
#   CommandNotFoundError: Your shell has not been properly configured to use 'conda activate'.
echo $'Calling: eval "$(conda shell.bash hook)"'
eval "$(conda shell.bash hook)"

conda activate medbert

# Slow tests are skipped by default, others can be run like so:
# (medbert) $ python -m unittest discover -vvv
python -m unittest discover -vvv

# Submit this job so:
#   sbatch sbatch_quick_tests.sh
