from estnltk.taggers import RegexTagger
from estnltk.taggers.tagger import Tagger
from typing import Sequence


class WeightChangeTagger(Tagger):
    """Tags weight changes.

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

        weight_loss_pattern = r"[^a-züõöäšž](?P<OBJECT>(alla|maha|lang|kaot|vähene|alane)[^,.!>]{,20}?)[^0-9,.!>](?P<VALUE>([1-9]?[0-9](\s?[,.],?\s?[0-9]+)?))\s*[Kk][Gg]"
        weight_loss_pattern2 = r"[^0-9,.!>](?P<VALUE>([1-9]?[0-9](\s?[,.],?\s?[0-9]+)?))\s*[Kk][Gg][^,.!>]{,20}?[^a-züõöäšž](?P<OBJECT>(alla|maha|lang|kaot|vähene|alane))"

        weight_gain_pattern = r"[^a-züõöäšž](?P<OBJECT>(juurde|juures|üles|tõus|suure|\+)[^,.!>]{,20}?)[^0-9,.!>](?P<VALUE>([1-9]?[0-9](\s?[,.],?\s?[0-9]+)?))\s*[Kk][Gg]"
        weight_gain_pattern2 = r"[^0-9,.!>](?P<VALUE>([1-9]?[0-9](\s?[,.],?\s?[0-9]+)?))\s*[Kk][Gg][^,.!>]{,20}?[^a-züõöäšž](?P<OBJECT>(juurde|juures|üles|tõus|suure))"

        voc = [
            {
                "regex_type": "KAALULANGUS",
                "_regex_pattern_": weight_loss_pattern,
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
                "REGEX_TYPE": "KAALULANGUS",
            },
            {
                "regex_type": "KAALULANGUS",
                "_regex_pattern_": weight_loss_pattern2,
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
                "REGEX_TYPE": "KAALULANGUS",
            },
            {
                "regex_type": "KAALUTÕUS",
                "_regex_pattern_": weight_gain_pattern,
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
                "REGEX_TYPE": "KAALUTÕUS",
            },
            {
                "regex_type": "KAALUTÕUS",
                "_regex_pattern_": weight_gain_pattern2,
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
                "REGEX_TYPE": "KAALUTÕUS",
            },
        ]

        self.output_attributes = output_attributes
        self.output_layer = output_layer
        self.input_layers = []
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
