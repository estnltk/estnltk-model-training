from tokenizers.implementations import BertWordPieceTokenizer
from tokenizers.pre_tokenizers import Whitespace


def create_vocabulary(model_path, files, size=8000, special_tokens=None, lowercase=False):
    """
    Creates a vocabulary by using the SentencePiece tokenizer in unigram mode.
    :param vocab_path: (The path with the name of the resulting vocabulary) type: std::string
    :param files: (list of input text files) type: [std::string],
    :param size: (vocabulary size)  type: int32 default: 8000
    :param special_tokens: (Special symbols). type: [std::string] default: ["[PAD]", "[UNK]", "[CLS]", "[SEP]", "[MASK]"]
    :param encoding: (The encoding of input and the resulting vocabulary): type: std::string default: 'utf-8'
    :param shuffle: (To shuffle the input sentences or not) type: bool default = True
    :param input_sentence_size: (maximum size of sentences the trainer loads) type: int32 default: 10000000 (10M)
    :param mode: (model algorithm: unigram, bpe, wordlevel or wordpiece)  type: std::string default: "unigram"
    :return: NoneType
    """
    if special_tokens is None:
        special_tokens = ["[PAD]", "[UNK]", "[CLS]", "[SEP]", "[MASK]"]

    tokenizer = BertWordPieceTokenizer(lowercase=lowercase)
    tokenizer.train(files, vocab_size=size, special_tokens=special_tokens)
    tokenizer.save_model(model_path)

