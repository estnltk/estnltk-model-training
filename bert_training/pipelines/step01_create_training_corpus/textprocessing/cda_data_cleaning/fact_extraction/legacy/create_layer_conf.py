"""Import and initialise here the EstNLTK taggers you want to use in the `CreateLayer` workflow.

"""
from typing import Dict

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction import EventTokenTagger
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction import (
    EventHeaderTagger,
)
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction import (
    EventSegmentsTagger,
)
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction.measurement_extraction.taggers import MeasurementTokenTagger
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction.measurement_extraction.taggers import MeasurementTagger

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction.procedure_splitting import SubcategoryTagger
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction.procedure_splitting import SubcategorySegmentsTagger
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction.procedure_splitting import StudyTagger
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction.procedure_splitting import PriceTagger
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction.procedure_splitting import StudyTextTagger
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction.procedure_splitting import DiagnosisTagger

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction import (
    EventTokenTagger as EventTokenTaggerDev,
)
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction import (
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
