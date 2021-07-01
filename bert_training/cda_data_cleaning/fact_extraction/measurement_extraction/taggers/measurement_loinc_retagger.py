import os
from estnltk import Tagger
from estnltk.taggers import Vocabulary


class MeasurementLoincRetagger(Tagger):
    """Adds `loinc_code` and `t_lyhend` attributes to the `measurements` layer.

    """

    conf_param = ["vocabulary"]

    def __init__(self):
        self.output_layer = "measurements"
        self.input_layers = [self.output_layer]
        self.output_attributes = [
            "name",
            "OBJECT",
            "VALUE",
            "UNIT",
            "MIN",
            "MAX",
            "DATE",
            "REGEX_TYPE",
            "loinc_code",
            "t_lyhend",
        ]

        file = os.path.join(os.path.dirname(__file__), "regextype_to_loinc.csv")
        self.vocabulary = Vocabulary.read_csv(file)

    def _make_layer(self, text: str, layers: dict, status: dict) -> None:
        layer = layers[self.input_layers[0]]
        layer.attributes = self.output_attributes
        mapping = self.vocabulary.mapping
        for span in layer:
            for annotation in span.annotations:
                loinc_code = None
                t_lyhend = None
                record = mapping.get(annotation.REGEX_TYPE)
                if record is not None:
                    loinc_code = record[0]["loinc_code"]
                    t_lyhend = record[0]["t_lyhend"]
                annotation.loinc_code = loinc_code
                annotation.t_lyhend = t_lyhend
        return layer
