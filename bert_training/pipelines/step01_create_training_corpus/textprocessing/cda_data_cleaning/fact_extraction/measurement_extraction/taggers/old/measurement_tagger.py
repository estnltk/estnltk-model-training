from estnltk.taggers import Tagger
from estnltk.taggers import CombinedTagger
from estnltk.taggers import SequentialTagger
from estnltk.taggers import FlattenTagger
from estnltk.taggers import GrammarParsingTagger
from estnltk.finite_grammar import Grammar

from . import BoneDensityTagger
from . import WeightChangeTagger


class MeasurementTagger(Tagger):
    """Parses measurements using grammar.

    """

    conf_param = ["tagger"]

    def __init__(self, output_layer="measurements", layer_of_tokens="measurement_tokens"):

        output_attributes = ["name", "OBJECT", "VALUE", "UNIT", "MIN", "MAX", "DATE", "REGEX_TYPE"]

        # TODO: better name for this tagger?
        tagger = GrammarParsingTagger(
            output_layer="parsed_measurements",
            layer_of_tokens=layer_of_tokens,
            attributes=output_attributes,
            output_nodes={"MEASUREMENT"},
            output_ambiguous=True,
            grammar=self.grammar(),
        )

        flatten_tagger = FlattenTagger(
            input_layer="parsed_measurements", output_layer="measurements", output_attributes=output_attributes
        )

        sequential_tagger = SequentialTagger(taggers=[tagger, flatten_tagger])

        bone_density_tagger = BoneDensityTagger(output_attributes=output_attributes, output_layer=output_layer)
        weight_change_tagger = WeightChangeTagger(output_attributes=output_attributes, output_layer=output_layer)

        self.tagger = CombinedTagger(
            output_layer=output_layer,
            output_attributes=output_attributes,
            taggers=[sequential_tagger, bone_density_tagger, weight_change_tagger],
        )

        self.input_layers = self.tagger.input_layers
        self.output_layer = self.tagger.output_layer
        self.output_attributes = self.tagger.output_attributes

    def _make_layer(self, text, layers, status):
        return self.tagger.make_layer(text=text, layers=layers, status=status)

    @staticmethod
    def unit_decorator(nodes):
        unit = ""
        number = ""
        for node in nodes:
            if node.name == "NUMBER":
                number = node["value"][0]
            elif node.name == "UNIT":
                unit = node["value"][0]
        return {"unit_type": nodes[-1]["unit_type"][0], "unit_text": unit, "number_text": number}

    @staticmethod
    def measurement_decorator(nodes):

        object_ = ""
        unit = ""
        value = ""
        date = ""
        type_ = ""
        min_ = ""
        max_ = ""

        for node in nodes:
            if node.name == "MO":
                object_ = node.text
                type_ = node["regex_type"][0]

            elif node.name == "QNUMBER":
                value = node["number_text"]
                unit = node["unit_text"]

            elif node.name == "DATE":
                date = node.text

        return {
            "unit_type": nodes[-1]["unit_type"],
            "OBJECT": object_,
            "UNIT": unit,
            "VALUE": value,
            "DATE": date,
            "REGEX_TYPE": type_,
            "MIN": min_,
            "MAX": max_,
        }

    @staticmethod
    def measurement_decorator2(nodes):
        object_ = ""
        unit = ""
        value = ""
        date = ""
        type_ = ""
        min_ = ""
        max_ = ""

        # for node in nodes:
        #    if node.name == 'MO':
        object_ = nodes[0].text
        type_ = nodes[0]["regex_type"][0]

        #    elif node.name == 'QNUMBER':
        value = nodes[1]["number_text"]
        unit = nodes[1]["unit_text"]

        if len(nodes) == 4:
            min_ = nodes[2]["number_text"]
            unit = nodes[2]["unit_text"]

            max_ = nodes[3]["number_text"]
            unit = nodes[3]["unit_text"]

        if len(nodes) == 3:

            max_ = nodes[2]["number_text"]
            unit = nodes[2]["unit_text"]

        #    elif node.name == 'DATE':
        #        date = node.text

        return {
            "unit_type": nodes[-1]["unit_type"],
            "OBJECT": object_,
            "UNIT": unit,
            "VALUE": value,
            "DATE": date,
            "REGEX_TYPE": type_,
            "MIN": min_,
            "MAX": max_,
        }

    @staticmethod
    def measurement_decorator3(nodes):
        object_ = ""
        unit = ""
        value = ""
        date = ""
        type_ = ""
        min_ = ""
        max_ = ""

        object_ = nodes[0].text
        type_ = nodes[0]["regex_type"][0]

        unit = nodes[1]["value"][0]

        if len(nodes) == 3:

            value = nodes[2]["number_text"]

        if len(nodes) == 5:

            min_ = nodes[2]["number_text"]
            max_ = nodes[3]["number_text"]
            value = nodes[4]["number_text"]

        return {
            "unit_type": nodes[-1]["unit_type"],
            "OBJECT": object_,
            "UNIT": unit,
            "VALUE": value,
            "DATE": date,
            "REGEX_TYPE": type_,
            "MIN": min_,
            "MAX": max_,
        }

    @staticmethod
    def measurement_validator(nodes):
        return nodes[-1]["unit_type"] != "time"

    def grammar(self):
        grammar = Grammar(
            start_symbols=["MEASUREMENT"],
            depth_limit=4,
            legal_attributes=[
                "value",
                "unit_type",
                "unit_text",
                "number_text",
                "OBJECT",
                "UNIT",
                "VALUE",
                "MIN",
                "MAX",
                "DATE",
                "REGEX_TYPE",
            ],
        )

        grammar.add_rule("QNUMBER", "NUMBER UNIT", group="g0", priority=0, decorator=self.unit_decorator)

        grammar.add_rule("QNUMBER", "NUMBER", group="g0", priority=1, decorator=self.unit_decorator)

        grammar.add_rule(
            "MEASUREMENT",
            "MO DATE QNUMBER",
            group="g1",
            priority=0,
            decorator=self.measurement_decorator,
            validator=self.measurement_validator,
        )

        grammar.add_rule(
            "MEASUREMENT",
            "DATE MO QNUMBER",
            group="g1",
            priority=0,
            decorator=self.measurement_decorator,
            validator=self.measurement_validator,
        )

        grammar.add_rule(
            "MEASUREMENT",
            "MO QNUMBER",
            group="g1",
            priority=1,
            decorator=self.measurement_decorator,
            validator=self.measurement_validator,
        )

        grammar.add_rule(
            "MEASUREMENT",
            "MO QNUMBER QNUMBER",
            group="g1",
            priority=0,
            decorator=self.measurement_decorator2,
            validator=self.measurement_validator,
        )

        grammar.add_rule(
            "MEASUREMENT",
            "MO QNUMBER QNUMBER QNUMBER",
            group="g1",
            priority=-1,
            decorator=self.measurement_decorator2,
            validator=self.measurement_validator,
        )

        grammar.add_rule(
            "MEASUREMENT",
            "MO UNIT QNUMBER QNUMBER QNUMBER",
            group="g1",
            priority=0,
            decorator=self.measurement_decorator3,
            validator=self.measurement_validator,
        )

        grammar.add_rule(
            "MEASUREMENT",
            "MO UNIT QNUMBER",
            group="g1",
            priority=1,
            decorator=self.measurement_decorator3,
            validator=self.measurement_validator,
        )

        return grammar
