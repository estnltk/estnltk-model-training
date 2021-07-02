from typing import List

from estnltk import Tagger, Layer


class StudyTextTagger(Tagger):
    """Tags study texts"""

    conf_param: List = []

    def __init__(self):
        self.input_layers: List = ["study", "pricecode"]
        self.output_layer = "study_text"
        self.output_attributes = ["STUDY_HEADER"]

    def _make_layer(self, text, layers, status):
        spans = sorted([*layers["study"], *layers["pricecode"]])
        layer = Layer("study_text", text_object=text, attributes=["STUDY_HEADER"])

        for a, b in zip(spans, spans[1:]):
            if a.layer.name == "study" and b.layer.name == "pricecode":
                assert a.end < b.start
                layer.add_annotation((a.end, b.start), STUDY_HEADER=a.layer.text[0].strip())

        return layer
