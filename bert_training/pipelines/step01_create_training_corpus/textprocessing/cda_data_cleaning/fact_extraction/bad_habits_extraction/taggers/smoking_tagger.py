from estnltk.taggers import RegexTagger
from estnltk.taggers.tagger import Tagger
from typing import Sequence
from estnltk import Text

class SmokingTagger(Tagger):
    """Tags smoking.
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
        output_layer: str = "smoking",
    ):

        smoking_pattern = r"([Ss]uitsetab|[Ss]uitsetaja|[Ss]uits(etamine)?[-.: ]*jah?|[Ss]uitsetanud)"
        smoking_pattern2 = r"([Ll]oobuda\s*suitsetamisest)"
        smoking_pattern3 = r"([Ss]uits(etamine)?[-.: ]*(kuni.{,20}päevas|harva|[0-9].{,20}päevas))"
        
        not_smoking_pattern = r"([Ee]i\s*suitseta|[Ss]uits(etamine)?[-.: ]*(ei|0)|[Ee]i\s*ole\s*suitsetanud|[Mm]ittesuitsetaja|[Ss]uitsetanud\s*ei\s*ole)"
        not_smoking_pattern2 = r"([Ss]uitsetamise\s*lõpeta[sn]|[Ll]õpetas\s*suitsetamise|[Ss]uitsu\s*ei\s*tee|[Ss]uitsetamisest\s*loobu[sn]|[Ll]oobus\s*suitsetamisest)"
        not_smoking_pattern3 = r"([Ee]ndine\s*suitsetaja|[Pp]ole\s*suitseta(nud|ja)|[Ss]uits,\s*alkohol,\s*narkootikumid[-.: ]*ei)"
        
        
        voc = [
            {
                "regex_type": "SMOKING_YES",
                "_regex_pattern_": smoking_pattern,
                "_group_": 0,
                "_priority_": 0,
                "_validator_": lambda m: True,
                "name": "SMOKING",
                "OBJECT": lambda m: m.group(0),
                "VALUE": lambda m: m.group(0).strip().lower(),
                "MIN": "",
                "MAX": "",
                "UNIT": "",
                "DATE": "",
                "SUBJECT": "",
                "REGEX_TYPE": "SMOKING_YES",
            },
            
            {
                "regex_type": "SMOKING_YES",
                "_regex_pattern_": smoking_pattern2,
                "_group_": 0,
                "_priority_": 0,
                "_validator_": lambda m: True,
                "name": "SMOKING",
                "OBJECT": lambda m: m.group(0),
                "VALUE": lambda m: m.group(0).strip().lower(),
                "MIN": "",
                "MAX": "",
                "UNIT": "",
                "DATE": "",
                "SUBJECT": "",
                "REGEX_TYPE": "SMOKING_YES",
            },
            
            {
                "regex_type": "SMOKING_YES",
                "_regex_pattern_": smoking_pattern3,
                "_group_": 0,
                "_priority_": 0,
                "_validator_": lambda m: True,
                "name": "SMOKING",
                "OBJECT": lambda m: m.group(0),
                "VALUE": lambda m: m.group(0).strip().lower(),
                "MIN": "",
                "MAX": "",
                "UNIT": "",
                "DATE": "",
                "SUBJECT": "",
                "REGEX_TYPE": "SMOKING_YES",
            },
            
            {
                "regex_type": "SMOKING_NO",
                "_regex_pattern_": not_smoking_pattern,
                "_group_": 0,
                "_priority_": 0,
                "_validator_": lambda m: True,
                "name": "SMOKING",
                "OBJECT": lambda m: m.group(0),
                "VALUE": lambda m: m.group(0).strip().lower(),
                "MIN": "",
                "MAX": "",
                "UNIT": "",
                "DATE": "",
                "SUBJECT": "",
                "REGEX_TYPE": "SMOKING_NO",
            },
                           {
                "regex_type": "SMOKING_NO",
                "_regex_pattern_": not_smoking_pattern2,
                "_group_": 0,
                "_priority_": 0,
                "_validator_": lambda m: True,
                "name": "SMOKING",
                "OBJECT": lambda m: m.group(0),
                "VALUE": lambda m: m.group(0).strip().lower(),
                "MIN": "",
                "MAX": "",
                "UNIT": "",
                "DATE": "",
                "SUBJECT": "",
                "REGEX_TYPE": "SMOKING_NO",
            },
                           {
                "regex_type": "SMOKING_NO",
                "_regex_pattern_": not_smoking_pattern3,
                "_group_": 0,
                "_priority_": 0,
                "_validator_": lambda m: True,
                "name": "SMOKING",
                "OBJECT": lambda m: m.group(0),
                "VALUE": lambda m: m.group(0).strip().lower(),
                "MIN": "",
                "MAX": "",
                "UNIT": "",
                "DATE": "",
                "SUBJECT": "",
                "REGEX_TYPE": "SMOKING_NO",
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
