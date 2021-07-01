from estnltk.taggers import Tagger
from estnltk.taggers import GrammarParsingTagger
from estnltk.finite_grammar import Grammar


class EventHeaderTagger(Tagger):
    """Parses measurements using grammar.

    """

    conf_param = ["tagger"]

    def __init__(self, output_layer="event_headers", layer_of_tokens="event_tokens"):

        self.tagger = GrammarParsingTagger(
            output_layer=output_layer,
            layer_of_tokens=layer_of_tokens,
            attributes=["DATE", "HEADERWORD", "DOCTOR", "DOCTOR_CODE", "SPECIALTY", "SPECIALTY_CODE", "ANONYM"],
            output_nodes={"EVENT"},
            output_ambiguous=True,
            grammar=self.grammar(),
        )

        self.input_layers = self.tagger.input_layers
        self.output_layer = output_layer
        self.output_attributes = self.tagger.output_attributes

    def _make_layer(self, text, layers, status):
        return self.tagger.make_layer(text=text, layers=layers, status=status)

    @staticmethod
    def event_decorator(nodes):
        date = ""
        headerword = ""
        anonym = []
        doctor = ""
        doctor_code = ""
        specialty = ""
        specialty_code = ""

        for node in nodes:
            if node.name == "DATE":
                date = node.text
            elif node.name == "HEADERWORD":
                headerword = node.text
            elif node.name == "ANONYM":
                anonym.append(node.text)
            elif node.name == "DOCTOR":
                doctor = node.text
            elif node.name == "DOCTOR_CODE":
                doctor_code = node.text
            elif node.name == "SPECIALTY":
                specialty = node["specialty"][0]
                specialty_code = node["specialty_code"][0]

        return {
            "DATE": date,
            "HEADERWORD": headerword,
            "ANONYM": anonym,
            "DOCTOR": doctor,
            "DOCTOR_CODE": doctor_code,
            "SPECIALTY": specialty,
            "SPECIALTY_CODE": specialty_code,
        }

    @staticmethod
    def measurement_validator(nodes):
        return nodes[-1]["unit_type"] != "time"

    def grammar(self):
        grammar = Grammar(
            start_symbols=["EVENT"],
            depth_limit=4,
            legal_attributes=["DATE", "HEADERWORD", "DOCTOR", "DOCTOR_CODE", "SPECIALTY", "SPECIALTY_CODE", "ANONYM"],
        )

        grammar.add_rule("EVENT", "DATE HEADERWORD", group="g0", priority=0, decorator=self.event_decorator)

        grammar.add_rule(
            "EVENT", "DATE DOCTOR DOCTOR_CODE SPECIALTY", group="g0", priority=0, decorator=self.event_decorator
        )

        grammar.add_rule("EVENT", "DATE ANONYM ANONYM", group="g0", priority=1, decorator=self.event_decorator)

        grammar.add_rule(
            "EVENT", "DATE ANONYM ANONYM DOCTOR_CODE SPECIALTY", group="g0", priority=0, decorator=self.event_decorator
        )

        grammar.add_rule("EVENT", "DATE", group="g0", priority=2, decorator=self.event_decorator)

        return grammar
