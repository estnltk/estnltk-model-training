# EstNLTK's Tagger for BERT-based morphological tagger

# Requirements:
# estnltk==1.7.3
# torch==2.4.0

# Code references:
# * https://github.com/estnltk/estnltk/blob/main/estnltk_neural/estnltk_neural/taggers/ner/estbertner_tagger.py
# * https://bitbucket.org/utDigiHum/public/src/8635582194ef1f2dcec53c40abdfa3f29299a067/skriptid/nimeuksuste_m2rgendamine/bert_morph_tagging_tagger.py

import os
import torch
import collections

from typing import MutableMapping

from transformers import AutoConfig, AutoTokenizer, AutoModelForTokenClassification

from estnltk import Text, Layer, Tagger

from bert_tokens_to_words_rewriter import BertTokens2WordsRewriter

class BertMorphTagger(Tagger):
    """Applies BERT-based morphological tagging"""

    def __init__(
        self,
        model_location: str,
        get_top_n_predictions: int = 1,
        output_layer: str = 'bert_morph_tagging',
        sentences_layer: str = 'sentences',
        words_layer: str = 'words',
        token_level: bool = False,
        **kwargs
    ):
        """
        Initializes BertMorphTagger

        Args:
            model_location (str): Path to model directory.
            get_top_n_predictions (int): Number of predictions for each word
            output_layer (str, optional): Name of the output named entity annotations layer. Defaults to 'bert_morph_tagging'.
            sentences_layer (str): Name of the layer containing sentences
            words_layer (str): Name of the layer containing words
            token_level (bool, optional): Tags the text BERT token-level or EstNLTK's word-level. Defaults to False.

        Raises:
            ValueError: Raises when <code>model_location</code> is not specified.
        """

        # Assert that model location exists
        assert model_location != ""
        if not os.path.exists(model_location):
            raise ValueError(f"Model location not found, model_location={model_location}")

        # Configuration parameters
        self.conf_param = ('get_top_n_predictions', 'bert_tokenizer', 'bert_morph_tagging', 'id2label', 'token_level', 'sentences_layer', 'words_layer', 'output_layer', 'input_layers', 'output_attributes')
        tokenizer_kwargs = { k:v for (k,v) in kwargs.items() if k in ['do_lower_case', 'use_fast'] }
        self.get_top_n_predictions = get_top_n_predictions
        self.bert_tokenizer = AutoTokenizer.from_pretrained(model_location, **tokenizer_kwargs )
        self.bert_morph_tagging = AutoModelForTokenClassification.from_pretrained(model_location,
                                                                        output_attentions = False,
                                                                        output_hidden_states = False)

        # Fetch id2label mapping from configuration
        config_dict = AutoConfig.from_pretrained(model_location).to_dict()
        self.id2label, _ = config_dict["id2label"], config_dict["label2id"]

        # Set input and output layers
        self.token_level = token_level
        self.sentences_layer = sentences_layer
        self.words_layer = words_layer
        self.output_layer = output_layer
        self.input_layers = [sentences_layer, words_layer]
        self.output_attributes = ['bert_tokens', 'morph_labels', 'probabilities']

    def _get_bert_morph_tagging_label_predictions(self, input_str, get_top_n_predictions = 1):
        """
        Applies Bert on the given input string and returns Bert's tokens,
        token indexes, and top N predicted labels for each token.

        Args:
            input_str (str): The input string to be processed.
            top_n (int): Number of top predictions to return for each token.

        Returns:
            list of dict: Each token's top N predictions with their probabilities.
        """
        # Tokenize the input string
        tokens, batch_encoding = self._tokenize_with_bert(input_str)
        token_indexes = torch.tensor([batch_encoding['input_ids']])

        # Check if the length exceeds the model's maximum sequence length
        max_seq_length = self.bert_tokenizer.model_max_length
        if token_indexes.size(1) > max_seq_length:
            raise ValueError(f"Input length exceeds the model's max_seq_length of {max_seq_length} tokens")

        # Get predictions
        with torch.no_grad():
            output = self.bert_morph_tagging(token_indexes)

        # Get top N predictions
        top_n_predictions = []
        logits = output.logits.squeeze()  # Shape: [sequence_length, num_labels]
        probs = torch.softmax(logits, dim=-1)  # Convert logits to probabilities

        for i, token_data in enumerate(tokens):
            token_probs = probs[i]  # Probabilities for the current token
            top_n_indices = torch.topk(token_probs, get_top_n_predictions).indices  # Top N label indices
            top_n_labels = [self.id2label[idx.item()] for idx in top_n_indices]  # Convert indices to labels
            top_n_probs = [round(token_probs[idx].item(), 5) for idx in top_n_indices]  # Get probabilities for top N labels

            top_n_predictions.append({
                'token': token_data,
                'predictions': [{'label': label, 'probability': prob} for label, prob in zip(top_n_labels, top_n_probs)]
            })

        return top_n_predictions

    def _tokenize_with_bert(self, text, include_spanless=True):
        """
        Tokenizes input string with Bert's tokenizer and returns a list of token spans.
        Each token span is a triple (start, end, token).
        If include_spanless==True (default), then Bert's special "spanless" tokens
        (e.g. [CLS], [SEP]) will also be included with their respective start/end indexes
        set to None.
        """
        tokens = []
        batch_encoding = self.bert_tokenizer(text)
        for token_id, token in enumerate(batch_encoding.tokens()):
            char_span = batch_encoding.token_to_chars(token_id)
            if char_span is not None:
                tokens.append( (char_span.start, char_span.end, token) )
            elif include_spanless:
                tokens.append( (None, None, token) )
        return tokens, batch_encoding

    def _make_layer(self, text: Text, layers: MutableMapping[str, Layer], status: dict) -> Layer:
        sentences_layer = layers[ self.sentences_layer ]
        words_layer = layers[ self.words_layer ]
        morph_layer = Layer(name=self.output_layer, 
                        attributes=self.output_attributes, 
                        text_object=text, 
                        parent=self.words_layer, 
                        ambiguous=True)

        for k, sentence in enumerate( sentences_layer ):
            sent_start = sentence.start
            sent_text  = sentence.enclosing_text
            # Apply batch processing: split larger input sentence into smaller chunks and process chunk by chunk
            sent_chunks, sent_chunk_indexes = _split_sentence_into_smaller_chunks(sent_text)
            for sent_chunk, (chunk_start, chunk_end) in zip(sent_chunks, sent_chunk_indexes):
                # Get predictions for the sentence
                top_n_predictions = self._get_bert_morph_tagging_label_predictions(sent_chunk, self.get_top_n_predictions)

                # Collect token level annotations (a label for each token)
                for token_data in top_n_predictions:
                    start, end  = token_data['token'][0], token_data['token'][1]
                    bert_tokens = token_data['token'][2]
                    if start is None or end is None:
                        continue  # Ignore sentence start and end tokens (<s>, </s>)
                    all_labels = [pred['label'] for pred in token_data['predictions']]
                    all_probabilities = [pred['probability'] for pred in token_data['predictions']]
                    token_span = (sent_start + chunk_start + start, sent_start + chunk_start + end)
                    for label, prob in zip(all_labels, all_probabilities):
                        annotation = {
                            'bert_tokens': bert_tokens,
                            'morph_labels': label,
                            'probabilities': prob
                        }
                        morph_layer.add_annotation(token_span, **annotation)

        # Add annotations
        if self.token_level:
            # Return token level annotations
            return morph_layer

        else:
            # Aggregate tokens back into words/phrases
            # Use BertTokens2WordsRewriter to convert BERT tokens to words
            rewriter = BertTokens2WordsRewriter(
                bert_tokens_layer=self.output_layer, 
                input_words_layer=self.words_layer, 
                output_attributes=self.output_attributes, 
                output_layer=self.output_layer, 
                decorator=rewriter_decorator)

            # Rewrite to align BERT tokens with words
            morph_layer = rewriter.make_layer(text, layers={morph_layer.name: morph_layer})

        assert len(morph_layer) == len(words_layer), \
        f"Failed to rewrite '{morph_layer.name}' layer tokens to '{words_layer.name}' layer words: {len(morph_layer)} != {len(words_layer)}"
        return morph_layer

def rewriter_decorator(text_obj, word_index, span):
    """
    Decorator function for <code>BertTokens2WordsRewriter</code>. \n
    Aggregates the <code>morph_labels</code> and <code>probabilities</code> from <code>shared_bert_tokens</code>, finds the most
    common top-1 label, and retrieves the top N labels and their probabilities from the
    first token that contains this top-1 label.

    Args:
        text_obj: EstNLTK Text object.
        words_index: Index of the word in <code>words</code> layer.
        span: Span

    Returns:
        dict: Annotations with the top N labels and probabilities for the word/phrase.
    """

    # Step 1: Find the most frequent top-1 label across all tokens
    top_1_label_counts = collections.Counter()

    for sp in span:
        top_1_label = sp['morph_labels'][0] # Get top-1 label
        top_1_label_counts[top_1_label] += 1  # Count occurrences of each top-1 label

    # Identify the most frequent top-1 label
    most_frequent_label = top_1_label_counts.most_common(1)[0][0]
    annotations = list()

    # Step 2: Find the first token that has this most frequent top-1 label
    for sp in span:
        if most_frequent_label in sp['morph_labels']:

            # Extract the top N labels and their probabilities starting from this label
            labels = sp['morph_labels']
            probabilities = sp['probabilities']

            assert len(labels) == len(probabilities)

            for (label, prob) in zip(labels, probabilities):
                annotation = {
                'bert_tokens': [sp['bert_tokens'][0] for sp in span],
                'morph_labels': label,
                'probabilities': prob
                }
                annotations.append(annotation)

            # Return the final annotation
            return annotations

    # Fallback if no label found (shouldn't happen)
    raise RuntimeError(f'Could not find a token with this label: {most_frequent_label}')

def _split_sentence_into_smaller_chunks(large_sent: str, max_size:int=1000, seek_end_symbols: str='.!?'):
    """
    Splits given large_sent into smaller texts following the text size limit.
    Each smaller text string is allowed to have at most `max_size` characters.
    Returns smaller text strings and their (start, end) indexes in the large_sent.
    """
    assert max_size > 0, f'(!) Invalid batch size: {max_size}'
    if len(large_sent) < max_size:
        return [large_sent], [(0, len(large_sent))]
    chunks = []
    chunk_separators = []
    chunk_indexes = []
    last_chunk_end = 0
    while last_chunk_end < len(large_sent):
        chunk_start = last_chunk_end
        chunk_end = chunk_start + max_size
        if chunk_end >= len(large_sent):
            chunk_end = len(large_sent)
        if isinstance(seek_end_symbols, str):
            # Heuristic: Try to find the last position in the chunk that
            # resembles sentence ending (matches one of the seek_end_symbols)
            i = chunk_end - 1
            while i > chunk_start + 1:
                char = large_sent[i]
                if char in seek_end_symbols:
                    chunk_end = i + 1
                    break
                i -= 1
        chunks.append( large_sent[chunk_start:chunk_end] )
        chunk_indexes.append( (chunk_start, chunk_end) )
        # Find next chunk_start, skip space characters
        updated_chunk_end = chunk_end
        if chunk_end != len(large_sent):
            i = chunk_end
            while i < len(large_sent):
                char = large_sent[i]
                if not char.isspace():
                    updated_chunk_end = i
                    break
                i += 1
            chunk_separators.append( large_sent[chunk_end:updated_chunk_end] )
        last_chunk_end = updated_chunk_end
    assert len(chunk_separators) == len(chunks) - 1
    # Return extracted chunks
    return ( chunks, chunk_indexes )