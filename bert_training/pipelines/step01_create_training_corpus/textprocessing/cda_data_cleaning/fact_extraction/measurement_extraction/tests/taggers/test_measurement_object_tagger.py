from pprint import pprint

from estnltk import Text
from cda_data_cleaning.fact_extraction.measurement_extraction.taggers import MeasurementObjectTagger


tagger = MeasurementObjectTagger(conflict_resolving_strategy="ALL")


def basic_test(annotation: str, text: str, expected: list):
    text = Text(text)
    tagger.tag(text)
    layer = text[tagger.output_layer]
    result = layer.text
    print(result)
    assert result == expected, annotation


def complete_test(annotation: str, text: str, expected: list):
    text = Text(text)
    tagger.tag(text)
    layer = text[tagger.output_layer]
    result = layer.to_records()
    pprint(result)
    assert result == expected, annotation


def test_1():
    basic_test("", "", [])


def test_2():
    complete_test("", "", [])
