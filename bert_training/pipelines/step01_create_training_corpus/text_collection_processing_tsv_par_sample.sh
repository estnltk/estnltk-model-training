#!/bin/bash
#SBATCH -J med_texts
#SBATCH -c 4
#SBATCH -N 1
#SBATCH --ntasks-per-node=1
#SBATCH --mem=8G
#SBATCH -t 48:00:00
##SBATCH --mail-type=ALL
##SBATCH --mail-user=meelis.perli@gmail.com


export TSV_IN=/gpfs/space/projects/stacc_health/data_for_mperli/egcut_epi_mperli_epi_texts_small.tsv
export TSV_OUT=/gpfs/space/projects/stacc_health/medBERT/cleaned_med_texts/egcut_epi_mperli_epi_texts_small_cleaned2.tsv
export TEMP_DIR=/gpfs/space/projects/stacc_health/medBERT/cleaned_med_texts/

module load any/python/3.8.3-conda
source activate py37 # an enironment, that has estnltk, pandas,

cd /gpfs/space/projects/stacc_health/medBERT/medbert/pipelines/step01_create_training_corpus
python text_collection_processing_tsv_par.py $TSV_IN $TSV_OUT $TEMP_DIR 4 4