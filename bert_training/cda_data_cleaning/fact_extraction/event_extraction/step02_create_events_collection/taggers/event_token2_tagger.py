from typing import List, MutableMapping

from estnltk import Text
from estnltk.taggers import MergeTagger
from estnltk.taggers import Tagger
from estnltk.layer.layer import Layer


def is_event_span(span):
    reg_type = span.regex_type[0]
    if reg_type:
        if reg_type.startswith("printout"):
            return False
    return True


def iterate_spans_conflicts(layer):
    """
    Iterates over all spans in a layer and returns two consecutive spans and if they are conflicting or not.
    Based on: https://github.com/estnltk/estnltk/blob/devel_1.6/estnltk/layer_operations/conflict_resolver.py#L13
    """
    len_layer = len(layer)
    for i, a in enumerate(layer):
        a_end = a.end
        for j in range(i + 1, len_layer):
            b = layer[j]
            if a_end <= b.start or (is_event_span(a) and is_event_span(b)):
                conflict = False
                yield conflict, a, b
                break
            # don't break, continue looking for conflicts for this a
            conflict = True
            yield conflict, a, b


class EventTokenConflictResolverTagger(Tagger):
    """
    Makes a copy of layer event tokens.
    Only that event token spans that are conflicting with printouts are excluded.
    """

    conf_param = ["merge_tagger"]

    input_layers: List[str] = []

    def __init__(
        self,
        input_layers=["printout_segments", "event_tokens"],
        output_attributes=("grammar_symbol", "unit_type", "value", "specialty_code", "specialty", "regex_type"),
        output_layer: str = "event_tokens2",
    ):
        self.input_layers = input_layers
        self.output_attributes = output_attributes
        self.output_layer = output_layer

        self.merge_tagger = MergeTagger(
            output_layer="merged_event_printout",
            input_layers=self.input_layers,
            output_attributes=self.output_attributes,
        )

    def _make_layer(self, text: Text, layers: MutableMapping[str, Layer], status: dict) -> Layer:
        new_layer = Layer(self.output_layer, attributes=self.output_attributes, text_object=text, ambiguous=True)
        # merge printout and event headers to one layer
        self.merge_tagger.tag(text)

        conflicting_span_list = []
        # iterate over all spans and return two consecutive spans (a,b) and if those are conflicting (conflict)
        for conflict, a, b in iterate_spans_conflicts(text.merged_event_printout):

            are_both_event = is_event_span(a) and is_event_span(b)
            # 1) no conflicts between events and printouts, only okay when both are events
            # 2) not a printout span 3) has never had any conflict
            if (not conflict or are_both_event) and is_event_span and (a.start, a.end) not in conflicting_span_list:
                # add only first element of the pair to the new layer, because second might still contain conflicts
                for annotation in a.annotations:
                    new_layer.add_annotation(a.base_span, **annotation)

            # save spans, where there was a conflict between event and printout
            if conflict and not are_both_event:
                conflicting_span_list.append((a.start, a.end))
                conflicting_span_list.append((b.start, b.end))

        # add last span
        last = text.merged_event_printout[-1]
        is_event = is_event_span(last)
        if is_event and (last.start, last.end) not in conflicting_span_list:
            for annotation in last.annotations:
                new_layer.add_annotation(last.base_span, **annotation)

        return new_layer
