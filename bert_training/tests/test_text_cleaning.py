import unittest

from estnltk import Text

from pipelines.step01_create_training_corpus.textprocessing.text_cleaning import extract_span_ranges, \
    reformat_sentences, clean_med, clean_med_r_events


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
        actual = clean_med(Text(text)).text
        self.assertEqual(expected, actual)

    def test_text_cleaning_date(self):
        text = "See on kirjutatud kuupäeval 30.06.2021."
        expected = "See on kirjutatud kuupäeval <DATE>."
        actual = clean_med(Text(text)).text
        self.assertEqual(expected, actual)

    def test_text_cleaning_event_header(self):
        text = "30.06.2021.\n Siin lauses peaks header olema"
        expected = "Siin lauses peaks header olema"
        actual = clean_med_r_events(Text(text))[0].text
        self.assertEqual(expected, actual)

    def test_text_cleaning_event_numbers(self):
        text = "See 2 lause 123.2 sisaldab 12.12 mõnda 13.13 numbrit 4,2."
        expected = "See <INT> lause <FLOAT> sisaldab <FLOAT> mõnda <FLOAT> numbrit <FLOAT>."
        actual = clean_med(Text(text)).text
        self.assertEqual(expected, actual)

    def test_next_line_symbol_swap(self):
        text = "\n See on rida\nsee on teine rida.\n"
        expected = "See on rida <br> see on teine rida."
        actual = clean_med(Text(text)).text
        self.assertEqual(expected, actual)

if __name__ == '__main__':
    unittest.main()
