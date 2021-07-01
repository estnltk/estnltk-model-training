import unittest

from pipelines.step01_create_training_corpus.textprocessing import tag_text
from pipelines.step01_create_training_corpus.textprocessing.text_cleaning import get_span_ranges, clean, \
    extract_sentences


def load_test_file(path):
    text = ""
    with open(path, encoding="utf-8") as file:
        for i in file:
            text += i
    return text


class textCleaningTestsCases(unittest.TestCase):


    def test_text_cleaning_clean(self):
        text = "See on katse lause."
        expected = "See on katse lause."
        actual = clean(text)
        self.assertEqual(expected, actual)

    def test_text_cleaning_date(self):
        text = "See on kirjutatud kuupäeval 30.06.2021."
        expected = "See on kirjutatud kuupäeval <DATE>."
        actual = clean(text)
        self.assertEqual(expected, actual)

    def test_text_cleaning_event_header(self):
        text = "30.06.2021.\n Siin lauses peaks header olema"
        expected = "Siin lauses peaks header olema"
        actual = clean(text)
        self.assertEqual(expected, actual)

    def test_text_cleaning_event_numbers(self):
        text = "See 2 lause 123.2 sisaldab 12.12 mõnda 13.13 numbrit 4,2."
        expected = "See <INT> lause <FLOAT> sisaldab <FLOAT> mõnda <FLOAT> numbrit <FLOAT>."
        actual = clean(text)
        self.assertEqual(expected, actual)


    def test_spans_after_merge(self):
        text = load_test_file(
            "../cda_data_cleaning/fact_extraction/event_extraction/step01_analysis_printout_extraction/example_texts/text6_3.txt")
        text = clean(text)
        extract_sentences(text)

        self.assertEqual(True, False)

if __name__ == '__main__':
    unittest.main()
