import os
from typing import Sequence
from estnltk.taggers import Tagger
from estnltk.taggers import RegexTagger
from estnltk.taggers import Vocabulary


vocabulary = os.path.join(os.path.dirname(__file__), "data/drug_name_regex_vocabulary_for_freetext.csv")
vocabulary = Vocabulary.read_csv(vocabulary_file=vocabulary)


class Precise4qDrugNameTagger(Tagger):
    """
    Tags occurreces of drugs defined in the scope of Precis4q study and nothing
    more. It does not decide whether the occurrece is positive or negative, i.e.,
    whether a patient uses or does not use a tagged drug.
    """

    conf_param = ["tagger"]

    def __init__(
        self,
        output_attributes: Sequence = ("grammar_symbol", "value", "_priority_"),
        conflict_resolving_strategy: str = "MAX",
        overlapped: bool = False,
        output_layer: str = "drug_names",
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
            ambiguous=False,
            ignore_case=True,
        )

    def _make_layer(self, text, layers, status):
        return self.tagger.make_layer(text=text, layers=layers, status=status)
