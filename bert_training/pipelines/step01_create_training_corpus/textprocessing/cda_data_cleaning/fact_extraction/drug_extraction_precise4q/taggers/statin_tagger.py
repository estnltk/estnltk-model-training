import os
from typing import Sequence
from estnltk.taggers import Tagger
from estnltk.taggers import RegexTagger
from estnltk.taggers import Vocabulary


vocabulary = os.path.join(os.path.dirname(__file__), "data/statin_tagger_voc.csv")
vocabulary = Vocabulary.read_csv(vocabulary_file=vocabulary)


class StatinTagger(Tagger):
    """
    Tags occurreces of statins (drug) and nothing more. It does not decide whether
    the occurrece is positive or negative, i.e., whether a patient uses or does
    not use a tagged drug.
    """

    conf_param = ["tagger"]

    def __init__(
        self,
        output_attributes: Sequence = (
            "grammar_symbol",
            "regex_type",
            "value",
            "correct_name",
            "substance",
            "_priority_",
        ),
        conflict_resolving_strategy: str = "MAX",
        overlapped: bool = True,
        output_layer: str = "statins",
    ):
        self.output_attributes = output_attributes
        self.output_layer = output_layer
        self.input_layers = []
        self.tagger = RegexTagger(
            vocabulary=vocabulary,
            output_layer=output_layer,
            output_attributes=output_attributes,
            conflict_resolving_strategy=conflict_resolving_strategy,
            priority_attribute="_priority_",
            overlapped=overlapped,
            ambiguous=True,
            ignore_case=True,
        )

    def _make_layer(self, text, layers, status):
        return self.tagger.make_layer(text=text, layers=layers, status=status)
