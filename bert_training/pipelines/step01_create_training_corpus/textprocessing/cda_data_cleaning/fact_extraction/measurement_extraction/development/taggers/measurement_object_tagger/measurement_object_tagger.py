from estnltk.taggers import RegexTagger
from estnltk.taggers import Tagger
from os import path
import csv

from .measurement_object_vocabulary import vocabulary as voc


class MeasurementObjectTagger(Tagger):
    """
    Tags measurement objects.
    """

    input_layers = []
    conf_param = ["tagger"]

    def __init__(
        self,
        output_attributes=("grammar_symbol", "regex_type", "value", "_priority_"),
        conflict_resolving_strategy="MAX",
        overlapped=True,
        output_layer="measurement_objects",
    ):

        with open(path.join(path.dirname(__file__), "loinc_names.csv"), "r") as fin:
            names = []
            reader = csv.reader(fin)
            header = next(reader)
            for row in reader:
                name = row[0]
                if "*" in name:
                    name = name.replace("*", "\*")
                names.append(name)

        before = "(^| |[^a-züõöäžšA-ZÜÕÖÄŽŠ])"
        after = "([^a-züõöäžšA-ZÜÕÖÄŽŠ]|$)"

        vocabulary = []

        for name in names:
            regex_pattern = before + "(" + name + ")" + after

            d = {
                "grammar_symbol": "MO",
                "regex_type": "name",
                "_regex_pattern_": regex_pattern,
                "_group_": 2,
                "_priority_": 1,
                "_validator_": lambda m: True,
                "value": lambda m: m.group(2),
            }

            vocabulary.append(d)

        for el in voc:
            temp = el["_regex_pattern_"]
            el["_regex_pattern_"] = before + "(" + temp + ")" + after
            el["_group_"] += 2
            vocabulary.append(el)

        self.output_attributes = output_attributes
        self.output_layer = output_layer
        self.tagger = RegexTagger(
            vocabulary=vocabulary,
            output_layer=output_layer,
            output_attributes=output_attributes,
            conflict_resolving_strategy=conflict_resolving_strategy,
            overlapped=overlapped,
            priority_attribute="_priority_",
        )

    def _make_layer(self, text, layers, status):
        return self.tagger.make_layer(text=text, layers=layers, status=status)
