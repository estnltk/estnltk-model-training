from pprint import pprint

from estnltk import Text
from cda_data_cleaning.fact_extraction.common.taggers.robust_date_number_tagger.robust_date_number_tagger import RobustDateNumberTagger


tagger = RobustDateNumberTagger(conflict_resolving_strategy="ALL")


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
    basic_test("", "26.05.2012 oli üks tore kuupäev", ['26.05.2012'])


def test_2():
    complete_test("", "", [])
