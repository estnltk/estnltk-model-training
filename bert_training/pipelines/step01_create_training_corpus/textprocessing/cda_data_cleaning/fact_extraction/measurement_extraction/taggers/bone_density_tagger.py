from estnltk.taggers import RegexTagger
from estnltk.taggers.tagger import Tagger
from typing import Sequence, List


class BoneDensityTagger(Tagger):
    """Tags bone density.

    """

    conf_param = ["tagger"]

    def __init__(
        self,
        output_attributes: Sequence = (
            "name",
            "OBJECT",
            "VALUE",
            "UNIT",
            "MIN",
            "MAX",
            "DATE",
            "REGEX_TYPE",
            "SUBJECT",
        ),
        conflict_resolving_strategy: str = "MAX",
        overlapped: bool = True,
        output_layer: str = "measurements",
    ):

        pattern = r"(?P<OBJECT>([Rr]eieluu |[Ll]ülisamba |[Nn]immeosa |[Nn]immepiirkonna )?(T| t|Z|z|[Rr]eieluu|[Ll]ülisamba|[Nn]immeosa)[- ]*skoor(iga)? ?(L1-L4|nimmeosas|nimmepiirkonnas|F|reieluus|on|lülisambas| |:)*)(?P<VALUE>([- ]*[0-9]+(\s?[,.],?\s?[0-9]+)?))"

        voc = [
            {
                "regex_type": "LUUTIHEDUS",
                "_regex_pattern_": pattern,
                "_group_": 0,
                "_priority_": 0,
                "_validator_": lambda m: True,
                "name": "MEASUREMENT",
                "OBJECT": lambda m: m.group("OBJECT"),
                "VALUE": lambda m: m.group("VALUE").strip().replace(",", "."),
                "MIN": "",
                "MAX": "",
                "UNIT": "",
                "DATE": "",
                "SUBJECT": "",
                "REGEX_TYPE": "LUUTIHEDUS",
            }
        ]

        self.output_attributes = output_attributes
        self.output_layer = output_layer
        self.input_layers: List = []
        self.tagger = RegexTagger(
            vocabulary=voc,
            output_attributes=output_attributes,
            conflict_resolving_strategy=conflict_resolving_strategy,
            # priority_attribute='_priority_',
            overlapped=overlapped,
            ambiguous=True,
            output_layer=output_layer,
        )

    def _make_layer(self, text, layers, status):
        return self.tagger.make_layer(text=text, layers=layers, status=status)
