# https://towardsdatascience.com/pre-training-bert-from-scratch-with-cloud-tpu-6e2f71028379
import os
import sys
# PARAMS

MAX_SEQ_LENGTH = 128 #@param {type:"integer"}
MASKED_LM_PROB = 0.15 #@param
MAX_PREDICTIONS = 20 #@param {type:"integer"}
DO_LOWER_CASE = False #@param {type:"boolean"}

PROJECT_DIR = '/gpfs/space/projects/stacc_health/bert_models/estmed_small/'
VOC_FNAME = 'vocab.txt'
PRETRAINING_DIR = 'data/pretraining/'
SHARD_DIR = 'data/train/'

n = str(sys.argv[1])
#n = "0"*(3-len(n)) + n
file = "train_" + n + ".txt"

# controls how many parallel processes xargs can create

# END OF PARAMS

VOC_FNAME = PROJECT_DIR + VOC_FNAME
PRETRAINING_DIR = PROJECT_DIR + PRETRAINING_DIR
SHARD_DIR = PROJECT_DIR + SHARD_DIR

XARGS_CMD = ("python /gpfs/space/projects/stacc_health/scripts/bertmod/create_pretraining_data.py "
             "--input_file={}{} "
             "--output_file={}{}.tfrecord "
             "--vocab_file={} "
             "--do_lower_case={} "
             "--max_predictions_per_seq={} "
             "--max_seq_length={} "
             "--masked_lm_prob={} "
             "--random_seed=123 "
             "--dupe_factor=5")

XARGS_CMD = XARGS_CMD.format(SHARD_DIR, file,PRETRAINING_DIR, file,
                             VOC_FNAME, DO_LOWER_CASE, 
                             MAX_PREDICTIONS, MAX_SEQ_LENGTH, MASKED_LM_PROB)


os.system("echo VOC_FNAME: " + VOC_FNAME + " ")                             
os.system("echo Shard_dir: " + SHARD_DIR + " ")
os.system("ls "+ SHARD_DIR + " ")
os.system("echo making dir: " + PRETRAINING_DIR + " ")
os.system("mkdir " + PRETRAINING_DIR + " ")

os.system(XARGS_CMD)