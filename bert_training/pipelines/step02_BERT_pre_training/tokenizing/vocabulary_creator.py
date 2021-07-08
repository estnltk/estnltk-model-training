import os
import sentencepiece as spm


def create_vocabulary(vocab_path, input_text_paths, size=8000, ctrl_symbols=None, encoding='utf-8',
                      shuffle=True, input_sentence_size=10000000, mode='unigram'):
    """
    Creates a vocabulary by using the SentencePiece tokenizer in unigram mode.
    :param vocab_path: (The path with the name of the resulting vocabulary) type: std::string
    :param input_text_paths: (path to input .txt files) type: std::string or [std::string],
    :param size: (vocabulary size)  type: int32 default: 8000
    :param ctrl_symbols: (Special symbols). type: [std::string] default: ["[PAD]", "[UNK]", "[CLS]", "[SEP]", "[MASK]"]
    :param encoding: (The encoding of input and the resulting vocabulary): type: std::string default: 'utf-8'
    :param shuffle: (To shuffle the input sentences or not) type: bool default = True
    :param input_sentence_size: (maximum size of sentences the trainer loads) type: int32 default: 10000000 (10M)
    :param mode: (model algorithm: unigram, bpe, word or char)  type: std::string default: "unigram"
    :return: NoneType
    """
    if ctrl_symbols is None:
        ctrl_symbols = ["[PAD]", "[UNK]", "[CLS]", "[SEP]", "[MASK]"]

    # A random file name
    model_prefix = "temp_vocab_creation_file_lb9fdf43ga46r1r"
    spm.SentencePieceTrainer.train(input=input_text_paths,
                                   model_prefix=model_prefix,
                                   vocab_size=size - len(ctrl_symbols),
                                   shuffle_input_sentence=shuffle,
                                   input_sentence_size=input_sentence_size,
                                   model_type=mode
                                   )

    snt_vocab = _read_sentencepiece_vocab(model_prefix, encoding)

    bert_vocab = list(map(_parse_sentencepiece_token, snt_vocab))

    bert_vocab = ctrl_symbols + bert_vocab

    bert_vocab += ["[UNUSED_{}]".format(i) for i in range(size - len(bert_vocab))]

    with open(vocab_path, "w", encoding=encoding) as fo:
        for token in bert_vocab:
            fo.write(token + "\n")


def _read_sentencepiece_vocab(model_prefix, encoding):
    voc = []
    with open("{}.vocab".format(model_prefix), encoding=encoding) as fi:
        for line in fi:
            voc.append(line.split("\t")[0])
    # skip the first <unk> token
    voc = voc[1:]
    # removing the model.model and model.vocab files
    os.remove("{}.vocab".format(model_prefix))
    os.system("echo Temporary file {}.vocab removed".format(model_prefix))

    os.remove("{}.model".format(model_prefix))
    os.system("echo Temporary file {}.model removed".format(model_prefix))

    return voc


def _parse_sentencepiece_token(token):
    if token.startswith("▁"):
        return token[1:]
    else:
        return "##" + token
