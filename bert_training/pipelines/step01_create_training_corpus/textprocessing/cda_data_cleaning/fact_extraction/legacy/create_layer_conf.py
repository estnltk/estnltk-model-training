"""Import and initialise here the EstNLTK taggers you want to use in the `CreateLayer` workflow.

"""
from typing import Dict

from cda_data_cleaning.fact_extraction.event_extraction.step02_create_events_collection.taggers import EventTokenTagger
from cda_data_cleaning.fact_extraction.event_extraction.step02_create_events_collection.taggers import (
    EventHeaderTagger,
)
from cda_data_cleaning.fact_extraction.event_extraction.step02_create_events_collection.taggers import (
    EventSegmentsTagger,
)
from cda_data_cleaning.fact_extraction.measurement_extraction.taggers import MeasurementTokenTagger
from cda_data_cleaning.fact_extraction.measurement_extraction.taggers import MeasurementTagger

from cda_data_cleaning.fact_extraction.procedure_splitting.development.taggers import SubcategoryTagger
from cda_data_cleaning.fact_extraction.procedure_splitting.development.taggers import SubcategorySegmentsTagger
from cda_data_cleaning.fact_extraction.procedure_splitting.development.taggers.study_tagger import StudyTagger
from cda_data_cleaning.fact_extraction.procedure_splitting.development.taggers.pricecode_tagger import PriceTagger
from cda_data_cleaning.fact_extraction.procedure_splitting.development.taggers.study_text_tagger import StudyTextTagger
from cda_data_cleaning.fact_extraction.procedure_splitting.development.taggers.diagnosis_tagger import DiagnosisTagger

from cda_data_cleaning.fact_extraction.event_extraction.step02_create_events_collection.development.taggers import (
    EventTokenTagger as EventTokenTaggerDev,
)
from cda_data_cleaning.fact_extraction.event_extraction.step02_create_events_collection.development import (
    EventTokenDiffTagger,
)


# map output_layer -> initialised tagger
layer_tagger = {
    "event_tokens": EventTokenTagger(output_layer="event_tokens"),
    "event_tokens_dev": EventTokenTaggerDev(output_layer="event_tokens_dev"),
    "event_tokens_diff": EventTokenDiffTagger(output_layer="event_tokens_diff"),
    "event_headers": EventHeaderTagger(),
    "event_segments": EventSegmentsTagger(),
    "subcategories": SubcategoryTagger(),
    "subcategory_segments": SubcategorySegmentsTagger(),
    "diagnosis": DiagnosisTagger(),
    "pricecode": PriceTagger(),
    "study": StudyTagger(output_layer="study"),
    "study_text": StudyTextTagger(),
    "measurement_tokens": MeasurementTokenTagger(output_layer="measurement_tokens"),
    "measurements": MeasurementTagger(),
}


layer_meta: Dict = {}
