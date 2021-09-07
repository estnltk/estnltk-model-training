import unittest

from pipelines.step01_text_processing.textprocessing.span_merging import mergeSpans


class span_testing(unittest.TestCase):
    def test_span_merging_simple(self):
        A = [{"start": 0, "end": 10}, {"start": 30, "end": 50}]
        B = [{"start": 15, "end": 20}]

        expected = [{"start": 0, "end": 10}, {"start": 15, "end": 20},
                    {"start": 30, "end": 50}]
        actual = mergeSpans(A, B)

        self.assertEqual(expected, actual)

    def test_span_merging_overlap(self):
        A = [{"start": 0, "end": 10}, {"start": 30, "end": 50}]
        B = [{"start": 5, "end": 20}]

        expected = [{"start": 0, "end": 10}, {"start": 30, "end": 50}]
        actual = mergeSpans(A, B)

        self.assertEqual(expected, actual)

    def test_span_merging_longA(self):
        A = [{"start": 50, "end": 200}]
        B = [{"start": 5, "end": 20}, {"start": 50, "end": 60},
             {"start": 75, "end": 100},
             {"start": 110, "end": 120}, {"start": 190, "end": 200},
             {"start": 201, "end": 220}]

        expected = [{"start": 5, "end": 20}, {"start": 50, "end": 200},
                    {"start": 201, "end": 220}]
        actual = mergeSpans(A, B)

        self.assertEqual(expected, actual)

    def test_span_merging_longB(self):
        B = [{"start": 50, "end": 200}]
        A = [{"start": 5, "end": 20}, {"start": 50, "end": 60},
             {"start": 75, "end": 100},
             {"start": 110, "end": 120}, {"start": 190, "end": 200},
             {"start": 201, "end": 220}]

        expected = [{"start": 5, "end": 20}, {"start": 50, "end": 60},
                    {"start": 75, "end": 100},
                    {"start": 110, "end": 120}, {"start": 190, "end": 200},
                    {"start": 201, "end": 220}]
        actual = mergeSpans(A, B)

        self.assertEqual(expected, actual)

    def test_span_merging_longB(self):
        B = [{"start": 50, "end": 200}]
        A = [{"start": 5, "end": 20}, {"start": 50, "end": 60},
             {"start": 75, "end": 100},
             {"start": 110, "end": 120}, {"start": 190, "end": 200},
             {"start": 201, "end": 220}]

        expected = [{"start": 5, "end": 20}, {"start": 50, "end": 60},
                    {"start": 75, "end": 100},
                    {"start": 110, "end": 120}, {"start": 190, "end": 200},
                    {"start": 201, "end": 220}]
        actual = mergeSpans(A, B)

        self.assertEqual(expected, actual)

    def test_span_merging_Edges(self):
        A = [{"start": 5, "end": 20},
             {"start": 201, "end": 220}]
        B = [{"start": 0, "end": 4},
             {"start": 230, "end": 250}]

        expected = [{"start": 0, "end": 4},
                    {"start": 5, "end": 20},
                    {"start": 201, "end": 220},
                    {"start": 230, "end": 250}]
        actual = mergeSpans(A, B)

        self.assertEqual(expected, actual)

    def test_span_merging_Big(self):
        A = [{"start": 5, "end": 20},
             {"start": 25, "end": 30},
             {"start": 55, "end": 60},
             {"start": 75, "end": 90},
             {"start": 150, "end": 175},
             {"start": 201, "end": 220}]
        B = [{"start": 0, "end": 4},
             {"start": 20, "end": 34},
             {"start": 40, "end": 50},
             {"start": 55, "end": 60},
             {"start": 70, "end": 74},
             {"start": 90, "end": 95},
             {"start": 111, "end": 123},
             {"start": 230, "end": 250}]

        expected = [{"start": 0, "end": 4},
                    {"start": 5, "end": 20},
                    {"start": 25, "end": 30},
                    {"start": 40, "end": 50},
                    {"start": 55, "end": 60},
                    {"start": 70, "end": 74},
                    {"start": 75, "end": 90},
                    {"start": 90, "end": 95},
                    {"start": 111, "end": 123},
                    {"start": 150, "end": 175},
                    {"start": 201, "end": 220},
                    {"start": 230, "end": 250}]
        actual = mergeSpans(A, B)

        self.assertEqual(expected, actual)


    def test_span_merging_empty(self):
        # only the second list is empty
        A = [{"start": 5, "end": 20},
             {"start": 25, "end": 30}]
        B = []

        expected = [{"start": 5, "end": 20},
                    {"start": 25, "end": 30}]
        actual = mergeSpans(A, B)

        self.assertEqual(expected, actual)

        # only the first list is empty
        B = [{"start": 5, "end": 20},
             {"start": 25, "end": 30}]
        A = []

        expected = [{"start": 5, "end": 20},
                    {"start": 25, "end": 30}]
        actual = mergeSpans(A, B)

        self.assertEqual(expected, actual)

        # Both are empty
        B = []
        A = []

        expected = []
        actual = mergeSpans(A, B)

        self.assertEqual(expected, actual)

if __name__ == '__main__':
    unittest.main()
