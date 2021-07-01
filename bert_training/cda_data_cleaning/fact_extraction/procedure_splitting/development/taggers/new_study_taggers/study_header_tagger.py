from estnltk import Tagger, Layer


class StudyHeaderTagger(Tagger):
    """Tags study texts"""

    conf_param = []

    def __init__(self):
        self.input_layers = ["study", "pricecode"]
        self.output_layer = "study_header"
        self.output_attributes = ["STUDY_HEADER"]

    def _make_layer(self, text, layers, status):
        spans = sorted([*layers["study"], *layers["pricecode"]])
        layer = Layer("study_header", text_object=text, attributes=["STUDY_HEADER"])

        # only study header e.g. "Teostatud uuring: RÖ-ülesvõte"  or "12.02.2020 Kompuutertomograafia"
        if (len(spans) == 1 and spans[0].layer.name == "study") or (
            len(spans) == 2 and spans[1].layer.name == "study"
        ):
            max_len = len(spans) - 1
            a = spans[max_len]
            layer.add_annotation((a.start, len(text.text)), STUDY_HEADER=a["study_general_header"][0])
            return layer

        for a, b in zip(spans, spans[1:]):
            # study header and price code e.g. "Teostatud uuring: Peaaju natiivis 7990"
            if a.layer.name == "study" and b.layer.name == "pricecode":
                if a.end < b.start:
                    layer.add_annotation((a.start, a.end), STUDY_HEADER=a["study_general_header"][0])

                # two only study headers e.g. "Teostatud uuring:RÖ-ülesvõte rindkerest PA Teostatud uuring: RÖ-ülesvõte rindkerest istudes"
            elif a.layer.name == "study" and b.layer.name == "study":
                if a.end < b.start:
                    layer.add_annotation((a.start, a.end), STUDY_HEADER=a["study_general_header"][0])

            # first one is tagged as pricecode (2010 which is actually year), second is study e.g. "23.09.2010 Kompuutertomograafia"
            # ignore the false pricecode
            elif a.layer.name == "pricecode" and b.layer.name == "study":
                if b.end < a.start:
                    layer.add_annotation(
                        (b.start, b.end), STUDY_HEADER=b["study_general_header"][0]
                    )  # originaal tekst, mis
        return layer
