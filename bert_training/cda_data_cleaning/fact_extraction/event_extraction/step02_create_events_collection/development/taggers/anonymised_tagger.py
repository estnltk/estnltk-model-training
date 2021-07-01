from typing import List

import regex as re
from estnltk import Tagger, Layer
from estnltk.layer.base_span import ElementaryBaseSpan


class AnonymisedTagger(Tagger):
    """Finds tags like <ANONYM id="8" type="per" morph="_H_ sg n"/>

    """

    conf_param = ["outer_pattern", "inner_pattern"]
    input_layers: List = []
    ANONYM_TAG_PATTERN = "<ANONYM(.*?)/>"

    def __init__(self, output_layer="anonymised"):
        self.output_layer = output_layer
        self.output_attributes = ["id", "type", "form", "partofspeech"]
        self.outer_pattern = re.compile(self.ANONYM_TAG_PATTERN)
        self.inner_pattern = re.compile(' id="(.*)" type="(.*)" (morph="(.*)")?')

    def _make_layer(self, text, layers, status):
        layer = Layer(name=self.output_layer, attributes=self.output_attributes, text_object=text, ambiguous=True)
        for match in self.outer_pattern.finditer(text.text):
            start, end = match.span(0)
            internal_text = match.group(1)
            internal_match = self.inner_pattern.match(internal_text)

            assert internal_match is not None, match.group(0)

            morphs = internal_match.group(4)
            if morphs is None:
                layer.add_annotation(
                    ElementaryBaseSpan(start, end),
                    id=internal_match.group(1),
                    type=internal_match.group(2),
                    form=None,
                    partofspeech=None,
                )
            else:
                for morph in morphs.split(";"):
                    layer.add_annotation(
                        ElementaryBaseSpan(start, end),
                        id=internal_match.group(1),
                        type=internal_match.group(2),
                        form=morph[1],
                        partofspeech=morph[4:8],
                    )
        return layer
