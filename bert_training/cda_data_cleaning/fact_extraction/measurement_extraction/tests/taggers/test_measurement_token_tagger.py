from pprint import pprint

from estnltk import Text
from cda_data_cleaning.fact_extraction.measurement_extraction.taggers import MeasurementTokenTagger


tagger = MeasurementTokenTagger()


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
    print()
    assert result == expected, annotation


def test_1():
    basic_test("", "PSA 2012. 1,53", ["PSA", "2012", "1,53"])
    basic_test("", "PSA 12. 53", ["PSA", "12. 53"])
    basic_test("", "PSA 12, 53", ["PSA", "12, 53"])
    basic_test("", "PSA 12 , 53", ["PSA", "12 , 53"])
    basic_test("", "PSA 2012.1,53", ["PSA", "2012", "1,53"])
    basic_test("", "PSA 20121,53", ["PSA", "2012", "1,53"])
    basic_test("", "PSA ,315", ["PSA", " ,315"])
    basic_test("", "PSA 030420121,53", ["PSA", "03042012", "1,53"])


def test_2():
    complete_test(
        "1",
        "PSA 2012. 1,53",
        [
            [{"end": 3, "grammar_symbol": "MO", "regex_type": "PSA", "start": 0, "unit_type": None, "value": "psa"}],
            [
                {
                    "end": 8,
                    "grammar_symbol": "DATE",
                    "regex_type": "date9",
                    "start": 4,
                    "unit_type": None,
                    "value": "partial_date",
                }
            ],
            [
                {
                    "end": 14,
                    "grammar_symbol": "NUMBER",
                    "regex_type": "anynumber",
                    "start": 10,
                    "unit_type": None,
                    "value": "1.53",
                }
            ],
        ],
    )

    complete_test(
        "2",
        "PSA 12. 53",
        [
            [{"end": 3, "grammar_symbol": "MO", "regex_type": "PSA", "start": 0, "unit_type": None, "value": "psa"}],
            [
                {
                    "end": 10,
                    "grammar_symbol": "NUMBER",
                    "regex_type": "anynumber",
                    "start": 4,
                    "unit_type": None,
                    "value": "12.53",
                }
            ],
        ],
    )

    complete_test(
        "3",
        "PSA 2012. 1,53",
        [
            [{"end": 3, "grammar_symbol": "MO", "regex_type": "PSA", "start": 0, "unit_type": None, "value": "psa"}],
            [
                {
                    "end": 8,
                    "grammar_symbol": "DATE",
                    "regex_type": "date9",
                    "start": 4,
                    "unit_type": None,
                    "value": "partial_date",
                }
            ],
            [
                {
                    "end": 14,
                    "grammar_symbol": "NUMBER",
                    "regex_type": "anynumber",
                    "start": 10,
                    "unit_type": None,
                    "value": "1.53",
                }
            ],
        ],
    )
