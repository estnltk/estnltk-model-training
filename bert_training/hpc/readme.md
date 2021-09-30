# Pre-training and fine-tuning the medBERT model on HPC

## Set-up 

1. Clone the repository into a desired location on the server (or use the one in `/gpfs/space/projects/stacc_health/data-egcut/medBERT/medbert`)
2. Set up the environment by running:
    ```
    module load any/python/3.8.3-conda
    conda env create -f medbert.yml
    ```
    For the workflows, be sure the environment is deactivated, as the script activates it itself:
    
    ```
    conda deactivate medbert
    ```

## Pre-training

1. Load the data into the folder [data/pretraining_data](data/pretraining_data). The data must be provided in the format of .tsv file or multiple files that follow the structure presented in [corp_res_clean_r_events_par.tsv](data/pretraining_data/corp_res_clean_r_events_par.tsv). The data is all in one column, the title of which is "text".

2. Run `pretrain_model.sh`:
   ```
   sbatch pretrain_model.sh
   ```
3. The output of the model will be in the folder `pretrained_model`, unless specified otherwise in the [run_config.ini](run_config.ini).

NB! The vocab size and maxlen are set to 4000 and 128 respectively. In the future, they will likely be customizable either via a BertConfig file or in the [run_config.ini](run_config.ini).


## Fine-tuning for sequence classification

1. Load the data into the folder [data/seq_training_data](data/seq_training_data). The data must be provided in the format of .tsv file or multiple files that follow the structure presented in [step03_seq_class_horisont_example.tsv](data/seq_training_data/step03_seq_class_horisont_example.tsv). The data consists of 2 columns, `text` and `y`, which functions as the label. 

2. Run `train_seq_model.sh`:
   ```
   sbatch train_seq_model.sh
   ```
3. The output of the model will be in the folder `seq_classifier_model`, unless specified otherwise in the [run_config.ini](run_config.ini).

## Fine-tuning for token classification

1. Load the data into the folder [data/token_training_data](data/token_training_data). The data must be provided in the format of .tsv file or multiple files that follow the structure presented in [step03_tok_class_horisont_example.tsv](data/token_training_data/step03_tok_class_horisont_example.tsv). The data consists of 2 columns, `text` and `y`. `text` are single words of a sentence with `y` being their label. The sentences are seperated by an empty line (consisting only of `""`).

2. Run `train_tok_model.sh`:
   ```
   sbatch train_tok_model.sh
   ```
3. The output of the model will be in the folder `token_classifier_model`, unless specified otherwise in the [run_config.ini](run_config.ini).


## Useful commands for HPC

`squeue -u <username>` - shows the jobs currently being run by the user.

`squeue -p gpu` - shows all the jobs running on the GPU.
