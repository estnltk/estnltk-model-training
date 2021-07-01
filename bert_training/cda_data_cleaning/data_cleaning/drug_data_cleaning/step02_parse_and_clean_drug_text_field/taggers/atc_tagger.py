from typing import Sequence, List
from estnltk.taggers import Tagger
from estnltk.taggers import RegexTagger

drug_code = [
    {
        "grammar_symbol": "ATC_CODE",
        "regex_type": "xx",
        "_regex_pattern_": r"(^| |-)?(?P<drug_code>[A-Z][0-9][0-9][A-Z][A-Z][0-9][0-9])($| |-|,)",
        "_group_": 0,
        "_priority_": 1,
        "_validator_": lambda m: True,
        "value": lambda m: m.group("drug_code"),
    }
]


class DrugCodeTagger(Tagger):
    """
    Tags ATC codes.
    """

    conf_param = ["tagger"]

    def __init__(
        self,
        output_attributes: Sequence = ("grammar_symbol", "regex_type", "value", "_priority_"),
        conflict_resolving_strategy: str = "MAX",
        overlapped: bool = True,
        output_layer: str = "drug_code",
    ):
        self.output_attributes = output_attributes
        self.output_layer = output_layer
        self.input_layers: List[str] = []
        self.tagger = RegexTagger(
            vocabulary=drug_code,
            output_attributes=output_attributes,
            conflict_resolving_strategy=conflict_resolving_strategy,
            priority_attribute="_priority_",
            overlapped=overlapped,
            ambiguous=False,
            ignore_case=True,
            output_layer="drug_code",
        )

    def _make_layer(self, text, layers, status):
        return self.tagger.make_layer(text=text, layers=layers, status=status)
