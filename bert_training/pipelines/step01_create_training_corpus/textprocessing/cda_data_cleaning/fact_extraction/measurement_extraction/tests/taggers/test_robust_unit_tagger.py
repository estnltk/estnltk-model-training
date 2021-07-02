from pprint import pprint

from estnltk import Text
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction.measurement_extraction.taggers import RobustUnitTagger


tagger = RobustUnitTagger(conflict_resolving_strategy="ALL")


def basic_test(annotation: str, text: str, expected: list):
    text = Text(text).tag_layer(["words"])
    tagger.tag(text)
    layer = text[tagger.output_layer]
    result = layer.text
    print(result)
    assert result == expected, annotation


def complete_test(annotation: str, text: str, expected: list):
    text = Text(text).tag_layer(["words"])
    tagger.tag(text)
    layer = text[tagger.output_layer]
    result = layer.to_records()
    pprint(result)
    assert result == expected, annotation


def test_0():
    basic_test("", "", [])
    basic_test("", "1 mm", ["mm"])


def test_1():
    basic_test("", "ng/mL ng/L", ["ng", "ng/mL", "mL", "ng", "ng/L"])


def test_2():
    complete_test(
        "",
        "5 mg/l, 3 lööki/s",
        [
            {
                "end": 4,
                "grammar_symbol": "UNIT",
                "regex_type": "simple_unit",
                "start": 2,
                "unit_type": "unit",
                "value": "mg",
            },
            {
                "end": 6,
                "grammar_symbol": "UNIT",
                "regex_type": "complex_unit",
                "start": 2,
                "unit_type": "unit",
                "value": "mg/l",
            },
            {
                "end": 6,
                "grammar_symbol": "UNIT",
                "regex_type": "simple_unit",
                "start": 5,
                "unit_type": "unit",
                "value": "l",
            },
            {
                "end": 15,
                "grammar_symbol": "UNIT",
                "regex_type": "word_unit",
                "start": 10,
                "unit_type": "unit",
                "value": "lööki",
            },
        ],
    )
