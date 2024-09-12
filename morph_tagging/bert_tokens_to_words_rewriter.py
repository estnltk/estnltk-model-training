#
# Rewrites/converts BERT tokens layer to EstNLTK's words layer. 
# The output layer is an enveloping layer around words layer. 
#

from typing import Dict, Any, List
import warnings

from estnltk import Text, Layer, Span, ElementaryBaseSpan, EnvelopingBaseSpan
from estnltk.taggers import Tagger

def default_decorator(text, sharing_words, shared_bert_tokens):
    return {}


class BertTokens2WordsRewriter(Tagger):
    '''Rewrites BERT tokens layer to a layer enveloping EstNLTK's words layer. 
       
       Annotations of the new layer are created by a decorator function, 
       which is applied on every pair (enveloped_words, corresponding_bert_tokens). 
       The decorator function has input parameters (text_obj, enveloped_words, 
       corresponding_bert_tokens). 
       The decorator function is expected to assign a single annotation to 
       each span of the layer (unambiguous output). Returned annotation should 
       be in form of a dictionary containing attributes & values. If decorator 
       returns None, then the annotation at that location is discarded. 
       If the decorator function is not specified, then default_decorator is 
       used, which returns {} on any input. 
    '''
    conf_param = ('bert_tokens_layer', 'decorator')

    def __init__(self, bert_tokens_layer: str, 
                       input_words_layer: str='words', 
                       output_attributes = (),
                       output_layer: str = None, 
                       decorator: callable = default_decorator):
        self.input_layers = [input_words_layer, bert_tokens_layer]
        self.output_layer = output_layer
        self.output_attributes = output_attributes
        if not callable(decorator):
            raise TypeError(f'(!) decorator should be a callable function, not {type(decorator)}')
        self.decorator = decorator

    def _make_layer_template(self):
        layer = Layer(name=self.output_layer,
                      attributes=self.output_attributes,
                      text_object=None,
                      parent=self.input_layers[0],
                      ambiguous=True )
        return layer

    def _make_layer(self, text, layers, status):
        words = layers[self.input_layers[0]]
        bert_tokens = layers[self.input_layers[1]]
        assert bert_tokens != self.output_layer, \
            ('cannot make new layer: input_layer and output_layer have the same name: {!r}. '+\
             '').format(self.output_layer)
        layer = self._make_layer_template()
        layer.text_object = text
        words_to_bert_tokens_map = map_words_to_bert_tokens(words, bert_tokens)
        for (k, v) in words_to_bert_tokens_map.items():
            new_span = text.words[k].base_span
            annotations = self.decorator(text, k, v)
            if annotations is not None:
                for annotation in annotations:
                    layer.add_annotation(new_span, annotation)
        return layer


def map_words_to_bert_tokens( words_layer:Layer, bert_tokens_layer:Layer, warn_unmatched:bool=True ):
    '''
    Finds a mapping from words to overlapping bert tokens. 
    Returns a dictionary where keys are indexes from the words_layer,
    and values are lists of entities from bert_tokens_layer.
    This function is based on the source:
      https://github.com/soras/vk_ner_lrec_2022/blob/main/data_preprocessing.py#L174C1-L222C32
    '''
    words_to_bert_tokens_map = dict()
    unmatched_bert_tokens = \
        set([j for j in range(len(bert_tokens_layer))])
    word_id = 0
    bert_tokens_start = 0
    while word_id < len( words_layer ):
        word    = words_layer[word_id]
        w_start = word.start
        w_end   = word.end
        matching_phrases = []
        i = bert_tokens_start
        while i < len(bert_tokens_layer):
            bert_tokens = bert_tokens_layer[i] # A single token or an enveloping tokens phrase
            if bert_tokens.start < w_end and w_end < bert_tokens.end:
                # inside bert_tokens and the next word also possibly overlaps bert_tokens
                '''
                bbbbbbbb
                  wwwww

                bbbbbbbb
                wwwww

                  bbbbbbbb
                wwwww
                '''
                if i in unmatched_bert_tokens:
                    unmatched_bert_tokens.remove(i)
                matching_phrases.append(bert_tokens)
                # Advance starting index
                bert_tokens_start = i
            elif bert_tokens.start <= w_end and w_start < bert_tokens.end and w_end >= bert_tokens.end:
                # inside bert_tokens and the next word cannot overlap bert_tokens
                '''
                bbbbbbbb
                wwwwwwww

                bbbbbbbb
                   wwwww

                bbbbbbbb
                      wwwww
                '''
                if i in unmatched_bert_tokens:
                    unmatched_bert_tokens.remove(i)
                matching_phrases.append(bert_tokens)
                # Advance starting index
                bert_tokens_start = i
            elif w_end < bert_tokens.start:
                # No need to look further
                break
            i += 1
        if matching_phrases:
            if word_id not in words_to_bert_tokens_map:
                words_to_bert_tokens_map[word_id] = []
            words_to_bert_tokens_map[word_id].extend( matching_phrases )
        word_id += 1
    if warn_unmatched and len(unmatched_bert_tokens) > 0:
        # Warn about bert unmatched tokens
        for i in sorted(list(unmatched_bert_tokens)):
            warnings.warn(f"(!) No matching {words_layer.name} span for bert token {bert_tokens_layer[i]}.")
    return words_to_bert_tokens_map