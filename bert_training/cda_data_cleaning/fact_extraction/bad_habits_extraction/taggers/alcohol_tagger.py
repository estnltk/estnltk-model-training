from estnltk.taggers import RegexTagger
from estnltk.taggers.tagger import Tagger
from typing import Sequence
from estnltk import Text

class AlcoholTagger(Tagger):
    """Tags alcohol drinking
    """

    conf_param = ["tagger"]

    def __init__(
        self,
        output_attributes: Sequence = (
            "name",
            "OBJECT",
            "VALUE"
        ),
        conflict_resolving_strategy: str = "MAX",
        overlapped: bool = True,
        output_layer: str = "alcohol",
    ):

        alcohol_pattern = r"(alkohoo?l|(^| )viin|(^| )õlu|vein|joobes|joobnud)"
        
        compress_pattern = r"(kompress|mähis|viinasokid|määri|hõõru|puhasta)"
        
        voc = [
            {
                "regex_type": "ALCOHOL",
                "_regex_pattern_": alcohol_pattern,
                "_group_": 0,
                "_priority_": 0,
                "_validator_": lambda m: True,
                "name": "ALCOHOL",
                "OBJECT": lambda m: m.group(0),
                "VALUE": lambda m: m.group(0).strip().lower()
            },
            
            {
                "regex_type": "COMPRESS",
                "_regex_pattern_": compress_pattern,
                "_group_": 0,
                "_priority_": 0,
                "_validator_": lambda m: True,
                "name": "COMPRESS",
                "OBJECT": lambda m: m.group(0),
                "VALUE": lambda m: m.group(0).strip().lower()
            }
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
