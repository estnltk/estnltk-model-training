from typing import Sequence, List
from estnltk.taggers import Tagger
from estnltk.taggers import RegexTagger
from .date_regex import regexes

# DATE
voc = []
for key, regex in regexes.items():
    d = {
        "grammar_symbol": "DATE",
        "regex_type": "date",
        "_regex_pattern_": regex.pattern,
        "_group_": 0,
        "_priority_": -1,
        "value": lambda m: m.group(0).strip(" ").strip(","),
    }
    voc.append(d)

# DRUG CODE
d = {
    "grammar_symbol": "ATC_CODE",
    "regex_type": "xx",
    "_regex_pattern_": r"(^| |-)?(?P<drug_code>[A-Z][0-9][0-9][A-Z][A-Z][0-9][0-9])($| |-|,)",
    "_group_": 0,
    "_priority_": -1,
    "value": lambda m: m.group("drug_code"),
}
#'_validator_': lambda m: True,
voc.append(d)

# RECIPE NUMBER
# is recipe number which consists of longer sequence of numbers (more than 10)
# or the recipe number starts with X followed by 7 numbers
d = {
    "grammar_symbol": "RECIPE_CODE",
    "regex_type": "recipe_code",
    "_regex_pattern_": "[0-9]{10,}|X [0-9]{7}",
    "_group_": 0,
    "_priority_": -1,
    "value": lambda m: m.group(0).strip(),
}
voc.append(d)

# PACKAGE SIZE
# how many tablets etc are in package
d = {
    "grammar_symbol": "PACKAGE_SIZE",
    "regex_type": "package_size",
    "_regex_pattern_": r"N(?:\w|\s{0})\d{1,4}",
    "_group_": 0,
    "_priority_": -1,
    "value": lambda m: m.group(0).strip(),
}
voc.append(d)

# FLOAT
# usually dose
d = {
    "grammar_symbol": "NUMBER",
    "regex_type": "number",
    "_regex_pattern_": r"\d+(,|\.)\d+",
    "_group_": 0,
    "_priority_": 0,
    "value": lambda m: m.group(0).strip(),
}
voc.append(d)

# INTEGER
# usually dose
d = {
    "grammar_symbol": "NUMBER",
    "regex_type": "number",
    "_regex_pattern_": r"\d+",
    "_group_": 0,
    "_priority_": 0,
    "value": lambda m: m.group(0).strip(),
}
voc.append(d)


# ratio
# ex. 80/12.5
# regex explanation:
#        first float second float or
#        first int second float or
#        first float second int or
#        first int second int or
d = {
    "grammar_symbol": "NUMBER",
    "regex_type": "number",
    "_regex_pattern_": r"(\d+(,|\.)\d+/\d+(,|\.)\d+|\d+/\d+(,|\.)\d+|\d+(,|\.)\d+/\d+|\d+/\d+)",
    "_group_": 0,
    "_priority_": 0,
    "value": lambda m: m.group(0).strip(),
}
voc.append(d)


class DrugDateNumberTagger(Tagger):
    """
    Tags event headers.
    """

    conf_param = ["tagger"]

    def __init__(
        self,
        output_attributes: Sequence = ("grammar_symbol", "regex_type", "value", "_priority_"),
        conflict_resolving_strategy: str = "MAX",
        overlapped: bool = False,
        output_layer: str = "dates_numbers",
    ):
        self.output_attributes = output_attributes
        self.output_layer = output_layer
        self.input_layers: List[str] = []
        self.tagger = RegexTagger(
            vocabulary=voc,
            output_attributes=output_attributes,
            conflict_resolving_strategy=conflict_resolving_strategy,
            priority_attribute="_priority_",
            overlapped=overlapped,
            ambiguous=False,
            output_layer="dates_numbers",
        )

    def _make_layer(self, text, layers, status):
        return self.tagger.make_layer(text=text, layers=layers, status=status)
