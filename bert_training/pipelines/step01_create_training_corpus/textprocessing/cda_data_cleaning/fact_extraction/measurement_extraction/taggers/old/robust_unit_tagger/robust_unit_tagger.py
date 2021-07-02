from estnltk.taggers import RegexTagger
from estnltk.taggers.tagger import Tagger
import csv
import regex as re
from os.path import dirname, join


class RobustUnitTagger(Tagger):
    """
    Tags units.
    """

    description = "Tags units."
    input_layers = ()
    conf_param = ["tagger"]

    def __init__(
        self,
        output_attributes=("grammar_symbol", "regex_type", "value", "unit_type"),
        conflict_resolving_strategy: str = "MAX",
        overlapped=True,
        output_layer="units",
    ):

        with open(join(dirname(__file__), "unit_vocabulary.csv"), "r") as fin:
            base_units = []
            complex_units = []
            reader = csv.reader(fin)
            header = next(reader)
            header = next(reader)
            for row in reader:
                unit = row[0]
                type_ = row[1]
                if "*" in unit:
                    unit = unit.replace("*", "\*")
                if "/" in unit:
                    complex_units.append((unit, type_))
                else:
                    base_units.append((unit, type_))

        with open(join(dirname(__file__), "unit_words_vocabulary.csv"), "r") as fin:
            word_units = []
            reader = csv.reader(fin)
            header = next(reader)
            header = next(reader)
            for row in reader:
                unit = row[0]
                type_ = row[1]
                word_units.append((unit, type_))

        before = "(^|[^a-züõöäžšA-ZÜÕÖÄŽŠ])"
        after = "([^a-züõöäžšA-ZÜÕÖÄŽŠ]|$)"
        vocabulary = []

        for unit in base_units:
            regex_pattern = before + "(" + unit[0] + ")" + after

            d = {
                "grammar_symbol": "UNIT",
                "regex_type": "simple_unit",
                "_regex_pattern_": regex_pattern,
                "_group_": 2,
                "_priority_": 1,
                "_validator_": lambda m: True,
                "value": lambda m: m.group(2),
                "unit_type": unit[1],
            }

            vocabulary.append(d)

        for unit in complex_units:
            unit2 = re.sub("/", " */ *", unit[0])
            regex_pattern = before + "(" + unit2 + ")" + after

            d = {
                "grammar_symbol": "UNIT",
                "regex_type": "complex_unit",
                "_regex_pattern_": regex_pattern,
                "_group_": 2,
                "_priority_": 1,
                "_validator_": lambda m: True,
                "value": lambda m: m.group(2).replace(" ", ""),
                "unit_type": unit[1],
            }

            vocabulary.append(d)

        for unit in word_units:
            regex_pattern = unit[0]

            d = {
                "grammar_symbol": "UNIT",
                "regex_type": "word_unit",
                "_regex_pattern_": regex_pattern,
                "_group_": 0,
                "_priority_": 1,
                "_validator_": lambda m: True,
                "value": lambda m: m.group(0),
                "unit_type": unit[1],
            }

            vocabulary.append(d)

        self.output_attributes = output_attributes
        self.output_layer = output_layer
        self.tagger = RegexTagger(
            vocabulary=vocabulary,
            output_attributes=output_attributes,
            conflict_resolving_strategy=conflict_resolving_strategy,
            overlapped=overlapped,
            output_layer=output_layer,
        )
        # priority_attribute='_priority_')

    def _make_layer(self, text, layers, status):
        return self.tagger.make_layer(text=text, layers=layers, status=status)
