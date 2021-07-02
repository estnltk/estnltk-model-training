import os
from typing import Sequence
from estnltk.taggers import Tagger
from estnltk.taggers import RegexTagger
from estnltk.taggers import Vocabulary


vocabulary = os.path.join(os.path.dirname(__file__), "drug_name_regex_vocabulary_for_freetext.csv")
# vocabulary = 'drug_name_regex_vocabulary.csv'
vocabulary = Vocabulary.read_csv(vocabulary_file=vocabulary)


class FreeTextDrugTagger(Tagger):
    """
    To be applied on free text fields, mainly
    ('summary', 'sumsum'),
    ('anamnesis', 'anamnesis'),
    ('anamnesis', 'anamsum'),
    ('anamnesis', 'dcase'),
    ('anamnesis', 'diagnosis'),
    ('summary', 'drug'),
    ('objective_finding', 'text'),
    ('surgery_entry', 'text'),
    ('surgery', 'text')
    """

    conf_param = ["tagger"]

    def __init__(
        self,
        output_attributes: Sequence = ("grammar_symbol", "value", "_priority_"),
        conflict_resolving_strategy: str = "MAX",
        overlapped: bool = False,
        output_layer: str = "drug",
    ):
        self.output_attributes = output_attributes
        self.output_layer = output_layer
        self.input_layers = []
        self.tagger = RegexTagger(
            vocabulary=vocabulary,
            output_attributes=output_attributes,
            conflict_resolving_strategy=conflict_resolving_strategy,
            priority_attribute="_priority_",
            overlapped=overlapped,
            ambiguous=False,
            ignore_case=True,
            output_layer="drug",
        )

    def _make_layer(self, text, layers, status):
        return self.tagger.make_layer(text=text, layers=layers, status=status)
