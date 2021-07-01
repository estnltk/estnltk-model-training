import sys
import sentencepiece as spm

# https://towardsdatascience.com/pre-training-bert-from-scratch-with-cloud-tpu-6e2f71028379

# PARAMS
PROJECT_DIR = '/gpfs/space/projects/stacc_health/'
VOC_FNAME = 'bert_models/estmed_2/vocab'
MODEL_PREFIX = 'estmed_2'
INPUT = 'bert_corpus/merged_truecased_forms.txt'
VOC_SIZE = 50000

# END OF PARAMS

VOC_FNAME = PROJECT_DIR + VOC_FNAME
MODEL_PREFIX = PROJECT_DIR + MODEL_PREFIX
INPUT = [PROJECT_DIR + i for i in INPUT]

def read_sentencepiece_vocab(filepath):
    voc = []
    with open(filepath, encoding='utf-8') as fi:
        for line in fi:
            voc.append(line.split("\t")[0])
    # skip the first <unk> token
    voc = voc[1:]
    return voc


def parse_sentencepiece_token(token):
    if token.startswith("▁"):
        return token[1:]
    else:
        return "##" + token

    
if __name__ == "__main__":
    spm.SentencePieceTrainer.train(input = INPUT,
                                   model_prefix = MODEL_PREFIX,
                                   vocab_size = VOC_SIZE,
                                   shuffle_input_sentence = True,
                                   input_sentence_size=10000000
                                  )


    snt_vocab = read_sentencepiece_vocab("{}.vocab".format(MODEL_PREFIX))    

    bert_vocab = list(map(parse_sentencepiece_token, snt_vocab))

    ctrl_symbols = ["[PAD]","[UNK]","[CLS]","[SEP]","[MASK]"]
    bert_vocab = ctrl_symbols + bert_vocab

    bert_vocab += ["[UNUSED_{}]".format(i) for i in range(VOC_SIZE - len(bert_vocab))]

    with open(VOC_FNAME, "w") as fo:
        for token in bert_vocab:
            fo.write(token+"\n")
        