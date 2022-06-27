#!/bin/sh

#SBATCH --partition=gpu
#SBATCH --ntasks=1
#SBATCH --mem=8G
#SBATCH --time=8:00:00
#SBATCH --gres=gpu:tesla:1
#SBATCH --exclude=falcon1

cd ..
source activate medbert
cd hpc

python pretrain_model.py
