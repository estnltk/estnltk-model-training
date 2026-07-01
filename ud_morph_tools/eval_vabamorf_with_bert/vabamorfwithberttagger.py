from estnltk import Text
import estnltk
from estnltk.taggers import Tagger
from estnltk import Span, Layer, Text
from estnltk.taggers.standard.morph_analysis.morf_common import IGNORE_ATTR
import json

from estnltk.taggers import VabamorfAnalyzer
from estnltk.taggers import PostMorphAnalysisTagger
from estnltk_neural.taggers import BertMorphTagger



class VabamorfWithBertTagger(Tagger):
    """
    Tagger that creates and refines 'morph_analysis' layer with
      1. VabamorfAnalyzer
      2. PostMorphAnalysisTagger
      3. BertMorphTagger
      and returns 'morph_analysis' or user defined layer. 
      PostMorphAnalysisTagger can be disabled using use_postanalysis=False (default True)
    """

    conf_param = ['use_postanalysis', 'vabamorf', 'post_morph', 'bert', 'output_layer', 'slang_lex', 'device']
    output_layer = 'morph_analysis'
    output_attributes = ['normalized_text', 'lemma', 'root', 'root_tokens', 'ending', 'clitic', 'form', 'partofspeech']
    input_layers = ()

    def __init__(self,
                post_morph=None,
                 vabamorf = None,
                 bert = None,
                 use_postanalysis=True,
                 output_layer=output_layer,
                 slang_lex=True,
                 device='cpu', # for gpu - 'cuda'
                ):

        self.output_layer = output_layer
        self.slang_lex = slang_lex
        self.device = device
        self.vabamorf = VabamorfAnalyzer(output_layer=self.output_layer)#, slang_lex=self.slang_lex)
        self.post_morph = PostMorphAnalysisTagger(output_layer=self.output_layer)
        self.bert = BertMorphTagger(output_layer=self.output_layer, disambiguate=True, device=self.device)
        self.use_postanalysis = use_postanalysis

       
    def _make_layer_template(self):
        return Layer(name=self.output_layer, attributes=self.output_attributes)
        
    
    def _make_layer(self, text, layers, status=None):

        # --------------------------------------------
        #   Morphological analysis
        # --------------------------------------------
        text.tag_layer( self.vabamorf.input_layers )
        morph_layer = self.vabamorf.make_layer( text, layers, status )

        # --------------------------------------------
        #   Adding necessary layers for post processing
        # -------------------------------------------- 
        layers2 = layers.copy()
        layers2["words"] = text["words"]
        layers2["sentences"] = text["sentences"]
        layers2["compound_tokens"] = text["compound_tokens"]
        layers2["tokens"] = text["tokens"]
        layers2[self.output_layer] = morph_layer
        
        # --------------------------------------------
        #   Post-processing after analysis
        # --------------------------------------------
        if self.use_postanalysis and self.post_morph:
            self.post_morph.change_layer( text, layers2, status )
            #  Remove _ignore from the output layer
            if IGNORE_ATTR in layers2[self.output_layer].attributes:
                layers2[self.output_layer].attributes = [attr for attr in layers2[self.output_layer].attributes if attr != IGNORE_ATTR]
                for span in layers2[self.output_layer]:
                    for annotation in span.annotations:
                        delattr(annotation, IGNORE_ATTR)
                        
        # --------------------------------------------
        #   Bert based analysis
        # --------------------------------------------
        self.bert.change_layer( text, layers2, status )

        return morph_layer



