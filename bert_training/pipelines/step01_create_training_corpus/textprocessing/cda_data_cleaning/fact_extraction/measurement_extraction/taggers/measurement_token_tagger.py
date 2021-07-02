from typing import List

from cda_data_cleaning.fact_extraction.common.taggers.robust_date_number_tagger.robust_date_number_tagger import RobustDateNumberTagger
from . import RobustUnitTagger
from . import MeasurementObjectTagger
from . import SubjectTagger

from estnltk.taggers import Tagger
from estnltk.taggers import SequentialTagger
from estnltk.taggers import GapTagger
from estnltk.taggers import MergeTagger


class MeasurementTokenTagger(Tagger):
    """Tags parts of measurements.

    """

    conf_param = ["tagger"]
    input_layers: List = []

    def __init__(
        self,
        output_attributes=("grammar_symbol", "unit_type", "value", "regex_type"),
        output_layer: str = "grammar_tags",
    ):

        self.output_attributes = output_attributes
        self.output_layer = output_layer

        date_num_tagger = RobustDateNumberTagger()

        unit_tagger = RobustUnitTagger()

        mo_tagger = MeasurementObjectTagger()

        subj_tagger = SubjectTagger()

        def gaps_decorator(text: str):
            return {"grammar_symbol": "RANDOM_TEXT_"}

        def trim(t: str) -> str:
            t = t.strip()
            t = t.strip("-")
            t = t.strip("=")
            t = t.strip("–")
            t = t.strip(":")
            t = t.strip(".")
            t = t.strip("|")
            t = t.strip("(")
            t = t.strip(")")
            t = t.strip("<")
            t = t.strip()

            # t = re.sub('oli', '', t)  # trim ei tohi muuta stringi keskosa: koolikulaadsed -> kokulaadsed
            return t

        gap_tagger = GapTagger(
            output_layer="gaps",
            input_layers=["dates_numbers", "measurement_objects", "units", "subject"],
            decorator=gaps_decorator,
            trim=trim,
            output_attributes=["grammar_symbol"],
            ambiguous=True,
        )

        merge_tagger = MergeTagger(
            output_layer=self.output_layer,
            input_layers=["dates_numbers", "measurement_objects", "units", "subject", "gaps"],
            output_attributes=self.output_attributes,
        )

        self.tagger = SequentialTagger(
            taggers=[date_num_tagger, mo_tagger, unit_tagger, subj_tagger, gap_tagger, merge_tagger]
        )

    def _make_layer(self, text, layers, status):
        return self.tagger.make_layer(text, layers, status)
