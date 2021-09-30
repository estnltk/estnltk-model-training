#!/bin/sh

#SBATCH --partition=gpu
#SBATCH --ntasks=1
#SBATCH --mem=8G
#SBATCH --time=8:00:00
#SBATCH --gres=gpu:tesla:1
#SBATCH --exclude=falcon1

cd /gpfs/space/projects/stacc_health/data-egcut/medBERT/medbert
source activate medbert
cd hpc

python train_tok_model.py
