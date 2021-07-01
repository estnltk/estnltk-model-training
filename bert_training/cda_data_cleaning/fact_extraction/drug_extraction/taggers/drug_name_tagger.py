import os
from typing import Sequence
from estnltk.taggers import Tagger
from estnltk.taggers import PhraseTagger
from estnltk.taggers import Vocabulary

vocabulary = os.path.join(os.path.dirname(__file__), "drug_vocabulary_v2.csv")
vocabulary = Vocabulary.read_csv(vocabulary_file=vocabulary)


class DrugNameTagger(Tagger):
    """
    Tags drug names and active ingredients.
    """

    conf_param = ["tagger"]

    def __init__(
        self,
        output_attributes: Sequence = ("grammar_symbol", "_phrase_", "value"),
        conflict_resolving_strategy: str = "MAX",
        overlapped: bool = True,
        output_layer: str = "drug_phrase",
    ):
        self.output_attributes = output_attributes
        self.output_layer = output_layer
        self.input_layers = []
        self.tagger = PhraseTagger(
            output_layer="drug_phrase",
            input_layer="words",
            input_attribute="text",
            vocabulary=vocabulary,
            key="_phrase_",
            output_ambiguous=True,
            ignore_case=True,
            output_attributes=["grammar_symbol", "_phrase_", "value"],
            conflict_resolving_strategy="MAX",
        )

    def _make_layer(self, text, layers, status):
        return self.tagger.make_layer(text=text, layers=layers, status=status)
