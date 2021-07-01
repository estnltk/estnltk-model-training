import os
from typing import Sequence, List

from estnltk.taggers import Tagger
from estnltk.taggers import RegexTagger
from estnltk.taggers import Vocabulary


vocabulary = os.path.join(os.path.dirname(__file__), "subcategories.csv")
vocabulary = Vocabulary.read_csv(vocabulary_file=vocabulary)


class SubcategoryTagger(Tagger):
    """Tags subcategories

    """

    conf_param = ["tagger"]

    def __init__(
        self,
        output_attributes: Sequence = ("grammar_symbol", "regex_type", "value", "_priority_"),
        conflict_resolving_strategy: str = "MAX",
        overlapped: bool = True,
        output_layer: str = "subcategories",
    ):
        self.output_attributes = output_attributes
        self.output_layer = output_layer
        self.input_layers: List[str] = []
        self.tagger = RegexTagger(
            vocabulary=vocabulary,
            output_attributes=output_attributes,
            conflict_resolving_strategy=conflict_resolving_strategy,
            priority_attribute="_priority_",
            overlapped=overlapped,
            ambiguous=True,
            ignore_case=True,
            output_layer="subcategories",
        )

    def _make_layer(self, text, layers, status):
        return self.tagger.make_layer(text=text, layers=layers, status=status)
