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
      and returns 'morph_analysis' layer. 
      PostMorphAnalysisTagger can be disabled using use_postanalysis=False (default True)
    """

    conf_param = ['use_postanalysis', 'vabamorf', 'post_morph', 'bert']
    output_layer = 'morph_analysis'
    output_attributes = ['normalized_text', 'lemma', 'root', 'root_tokens', 'ending', 'clitic', 'form', 'partofspeech', '_ignore']
    input_layers = ()

    def __init__(self,
                post_morph=None,
                 vabamorf = None,
                 bert = None,
                 use_postanalysis=True,):

        self.vabamorf = VabamorfAnalyzer()
        self.post_morph = PostMorphAnalysisTagger()
        self.bert = BertMorphTagger(output_layer='morph_analysis', disambiguate=True)
        self.use_postanalysis = use_postanalysis

        
    def _make_layer(self, text, layers, status=None):
        """
        For Tagger implementation. Calls 'tag' function.
        """
        return self.tag(text, layers=layers)
    

    def tag(self, text, layers=None):
        """
        Runs all the taggers, refines 'morph_analysis' layer and returns it.
        """

        text.tag_layer( self.vabamorf.input_layers )
        # Vabamorf and initial morph_analysis layer
        self.vabamorf.tag(text)

        # Post-processing morph_analysis
        if self.use_postanalysis:
            self.post_morph.retag(text)

            #  Remove _ignore from the output layer
            if IGNORE_ATTR in text["morph_analysis"].attributes:
                text["morph_analysis"].attributes = [attr for attr in text["morph_analysis"].attributes if attr != IGNORE_ATTR]
                for span in text["morph_analysis"]:
                    for annotation in span.annotations:
                        delattr(annotation, IGNORE_ATTR)

        # BERT-based corrections
        self.bert.retag(text)

        return text["morph_analysis"]

