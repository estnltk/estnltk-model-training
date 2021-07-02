import luigi

from estnltk.layer_operations import split_by
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import CDAJob, CDASubtask

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction import CreateLayer
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction import EventSegmentsTagger, EventHeaderTagger, EventTokenTagger
from .create_procedure_texts_collection import CreateProcedureTextsCollection, PROCEDURE_TEXTS_COLLECTION


PROCEDURE_ENTRIES_COLLECTION = "procedure_entries"
EVENT_SEGMENTS_LAYER = "event_segments"


class CreateProcedureEntriesCollection(CDAJob):
    """Extract procedure text fields from the source tables and create an EstNLTK <prefix>_<TEXTS_COLLECTION>
    collection.

    """

    prefix = luigi.Parameter()
    config_file = luigi.Parameter()

    def requires(self):

        task_00 = self.requirement

        create_procedure_texts = CreateProcedureTextsCollection(
            prefix=self.prefix,
            config_file=self.config_file,
            requirement=task_00
        )

        task_01 = CreateEmptyProcedureEntriesCollection(self.prefix, self.config_file, requirement=create_procedure_texts)

        task_02 = CreateLayer(
            prefix=self.prefix,
            config_file=self.config_file,
            work_schema=self.work_schema,
            role=self.role,
            collection=PROCEDURE_TEXTS_COLLECTION,
            layer="event_tokens",
            tagger=EventTokenTagger(output_layer="event_tokens"),
            requirement=task_01,
            )

        task_03 = CreateLayer(
            prefix=self.prefix,
            config_file=self.config_file,
            work_schema=self.work_schema,
            role=self.role,
            collection=PROCEDURE_TEXTS_COLLECTION,
            layer="event_headers",
            tagger=EventHeaderTagger(),
            requirement=task_02,
        )

        task_04 = CreateLayer(
            prefix=self.prefix,
            config_file=self.config_file,
            work_schema=self.work_schema,
            role=self.role,
            collection=PROCEDURE_TEXTS_COLLECTION,
            layer=EVENT_SEGMENTS_LAYER,
            tagger=EventSegmentsTagger(),
            requirement=task_03,
        )

        return [
            task_00, create_procedure_texts, task_01, task_02, task_03, task_04
        ]

    def run(self):

        storage = self.create_estnltk_storage(self.config_file)

        input_collection = storage[str(self.prefix) + "_" + PROCEDURE_TEXTS_COLLECTION]

        output_collection = storage[str(self.prefix) + "_" + PROCEDURE_ENTRIES_COLLECTION]
        output_collection.column_names = [
            "id",
            "data",
            "texts_id",
            "epi_id",
            "epi_type",
            "schema",
            "table",
            "field",
            "row_id",
            "effective_time",
            "header",
            "header_offset",
            "event_offset",
            "event_header_date",
            "doctor_code",
            "specialty",
            "specialty_code",
        ]

        with output_collection.insert() as insert:
            for id_, input_text, meta in input_collection.select(
                collection_meta=["epi_id", "epi_type", "schema", "table", "field", "row_id", "effective_time"],
                layers=[EVENT_SEGMENTS_LAYER, "anonymised"],
            ):
                texts = split_by(text=input_text, layer=EVENT_SEGMENTS_LAYER, layers_to_keep=["anonymised"])
                for text, segment_span in zip(texts, input_text[EVENT_SEGMENTS_LAYER]):
                    metadata = meta
                    metadata["texts_id"] = id_

                    metadata["header"] = segment_span.header
                    metadata["header_offset"] = segment_span.header_offset
                    metadata["event_header_date"] = segment_span.DATE
                    metadata["doctor_code"] = segment_span.DOCTOR_CODE
                    metadata["specialty"] = segment_span.SPECIALTY
                    metadata["specialty_code"] = segment_span.SPECIALTY_CODE

                    metadata["event_offset"] = segment_span.start

                    insert(text, meta_data=metadata)

        storage.close()

        self.mark_as_complete()


class CreateEmptyProcedureEntriesCollection(CDASubtask):
    """Create an empty collection.

    """

    prefix = luigi.Parameter()
    config_file = luigi.Parameter()
    requirement = luigi.TaskParameter()

    def requires(self):
        return [self.requirement]

    def run(self):

        storage = self.create_estnltk_storage(self.config_file)

        collection = storage[str(self.prefix) + "_" + PROCEDURE_ENTRIES_COLLECTION]
        collection.create(
            meta={
                "texts_id": "int",
                "epi_id": "str",
                "epi_type": "str",
                "schema": "str",
                "table": "str",
                "field": "str",
                "row_id": "str",
                "effective_time": "datetime",
                "header": "str",
                "header_offset": "int",
                "event_offset": "int",
                "event_header_date": "str",
                "doctor_code": "str",
                "specialty": "str",
                "specialty_code": "str",
            }
        )

        storage.close()

        self.mark_as_complete()
