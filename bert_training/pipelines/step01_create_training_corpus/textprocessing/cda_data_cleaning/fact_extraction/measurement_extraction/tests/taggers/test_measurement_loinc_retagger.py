from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction.measurement_extraction.taggers import MeasurementLoincRetagger


def test_measurement_loinc_retagger():
    retagger = MeasurementLoincRetagger()
    # TODO
