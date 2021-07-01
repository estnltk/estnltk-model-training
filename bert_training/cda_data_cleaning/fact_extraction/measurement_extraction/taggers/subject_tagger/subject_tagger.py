from estnltk.taggers import RegexTagger
from estnltk.taggers.tagger import Tagger
from typing import Sequence, List

from .subject_vocabulary import vocabulary as voc


class SubjectTagger(Tagger):
    """Tags measurement subjects.

    """

    conf_param = ["tagger"]

    def __init__(
        self,
        output_attributes: Sequence = ("grammar_symbol", "regex_type", "value", "_priority_"),
        conflict_resolving_strategy: str = "MAX",
        overlapped: bool = True,
        output_layer: str = "subject",
    ):

        self.output_attributes = output_attributes
        self.output_layer = output_layer
        self.input_layers: List = []
        self.tagger = RegexTagger(
            vocabulary=voc,
            output_attributes=output_attributes,
            conflict_resolving_strategy=conflict_resolving_strategy,
            priority_attribute="_priority_",
            overlapped=overlapped,
            ambiguous=True,
            output_layer=output_layer,
        )

    def _make_layer(self, text, layers, status):
        return self.tagger.make_layer(text=text, layers=layers, status=status)
