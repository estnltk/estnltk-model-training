from typing import List

from estnltk.taggers import Tagger

from .drug_name_tagger import DrugNameTagger
from .drug_date_number_tagger import DrugDateNumberTagger
from .unit_tagger.robust_unit_tagger import RobustUnitTagger
from estnltk.taggers import MergeTagger, FlattenTagger


class DrugGrammarTokenTagger(Tagger):
    """
    Tags drug field parts.
    """

    conf_param = ["drug_name_tagger", "date_number_tagger", "unit_tagger", "flatten_tagger", "merge_tagger"]

    def __init__(
        self,
        output_attributes=("grammar_symbol", "value"),
        # conflict_resolving_strategy: str = 'MAX',
        # overlapped: bool = True,
        output_layer: str = "grammar_tags",
    ):
        self.input_layers: List[str] = []  # ['words']
        self.output_attributes = tuple(output_attributes)
        self.output_layer = output_layer
        # priority_attribute = '_priority_'

        self.drug_name_tagger = DrugNameTagger()

        self.date_number_tagger = DrugDateNumberTagger()

        self.unit_tagger = RobustUnitTagger()

        self.flatten_tagger = FlattenTagger(
            input_layer="drug_phrase",
            output_layer="drug_phrase_flat",
            output_attributes=["grammar_symbol", "value", "_priority_"],
        )

        self.merge_tagger = MergeTagger(
            output_layer="grammar_tags",
            input_layers=["dates_numbers", "drug_phrase_flat", "units"],
            output_attributes=["grammar_symbol", "value"],
        )

    def _make_layer(self, text, layers, status):
        tmp_layers = {}
        tmp_layers["drug_phrase"] = self.drug_name_tagger.make_layer(text=text, layers=tmp_layers, status=status)
        tmp_layers["dates_numbers"] = self.date_number_tagger.make_layer(text=text, layers=tmp_layers, status=status)
        tmp_layers["units"] = self.unit_tagger.make_layer(text=text, layers=tmp_layers, status=status)
        tmp_layers["drug_phrase_flat"] = self.flatten_tagger.make_layer(text=text, layers=tmp_layers, status=status)

        return self.merge_tagger.make_layer(text=text, layers=tmp_layers, status=status)
