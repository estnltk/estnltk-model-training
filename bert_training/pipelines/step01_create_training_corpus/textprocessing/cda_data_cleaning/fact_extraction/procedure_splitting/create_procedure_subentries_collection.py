import luigi
import os

from estnltk.layer_operations import split_by

from cda_data_cleaning.common import get_postgres_storage
from cda_data_cleaning.common.luigi_targets import luigi_targets_folder
from cda_data_cleaning.fact_extraction.legacy import CreateLayer
from .create_procedure_entries_collection import CreateProcedureEntriesCollection, PROCEDURE_ENTRIES_COLLECTION


PROCEDURE_SUBENTRIES_COLLECTION = "procedure_subentries"
PROCEDURE_SEGMENTS_LAYER = "subcategory_segments"


class CreateProcedureSubentriesCollection(luigi.Task):
    """Tag events in the `<prefix>_<TEXTS_COLLECTION>` collection
    and create a separate `<prefix>_<SEGMENTS_COLLECTION>` collection.

    """

    prefix = luigi.Parameter()
    config_file = luigi.Parameter()

    def requires(self):
        return [
            CreateLayer(
                self.prefix,
                self.config_file,
                PROCEDURE_ENTRIES_COLLECTION,
                PROCEDURE_SEGMENTS_LAYER,
                CreateProcedureEntriesCollection(self.prefix, self.config_file),
            ),
            CreateEmptyProcedureSubentriesCollection(self.prefix, self.config_file),
        ]

    def run(self):
        with get_postgres_storage(self.config_file) as storage:
            input_collection = storage[str(self.prefix) + "_" + PROCEDURE_ENTRIES_COLLECTION]

            output_collection = storage[str(self.prefix) + "_" + PROCEDURE_SUBENTRIES_COLLECTION]
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
                    layers=[PROCEDURE_SEGMENTS_LAYER, "anonymised"],
                ):
                    texts = split_by(text=input_text, layer=PROCEDURE_SEGMENTS_LAYER, layers_to_keep=["anonymised"])
                    # sections = [(span.start, span.end) for span in input_text[PROCEDURE_SEGMENTS_LAYER]]
                    # texts = extract_sections(text=input_text, sections=sections, layers_to_keep=['anonymised'])
                    for text, segment_span in zip(texts, input_text[PROCEDURE_SEGMENTS_LAYER]):
                        metadata = meta
                        # metadata['texts_id'] = id_

                        # metadata['header'] = segment_span.header
                        # metadata['header_offset'] = segment_span.header_offset
                        # metadata['event_header_date'] = segment_span.DATE
                        # metadata['doctor_code'] = segment_span.DOCTOR_CODE
                        # metadata['specialty'] = segment_span.SPECIALTY
                        # metadata['specialty_code'] = segment_span.SPECIALTY_CODE

                        # metadata['event_offset'] = segment_span.start
                        metadata["subcategory"] = segment_span.subcategory

                        insert(text, meta_data=metadata)

        os.makedirs(luigi_targets_folder(self.prefix, self.config_file, self), exist_ok=True)
        with self.output().open("w"):
            pass

    def output(self):
        folder = luigi_targets_folder(self.prefix, self.config_file, self)
        return luigi.LocalTarget(
            os.path.join(folder, "create_{}_collection_done".format(PROCEDURE_SUBENTRIES_COLLECTION))
        )


class CreateEmptyProcedureSubentriesCollection(luigi.Task):
    """Create an empty EstNLTK collection `<prefix>_events` for the events.

    """

    prefix = luigi.Parameter()
    config_file = luigi.Parameter()

    def run(self):
        with get_postgres_storage(self.config_file) as storage:
            collection = storage[str(self.prefix) + "_" + PROCEDURE_SUBENTRIES_COLLECTION]
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

        os.makedirs(luigi_targets_folder(self.prefix, self.config_file, self), exist_ok=True)
        with self.output().open("w"):
            pass

    def output(self):
        folder = luigi_targets_folder(self.prefix, self.config_file, self)
        return luigi.LocalTarget(
            os.path.join(folder, "create_empty_collection_{}_done".format(PROCEDURE_SUBENTRIES_COLLECTION))
        )


class CreateStudyLayerInProcedureSubentries(luigi.Task):
    """

    """

    prefix = luigi.Parameter()
    config_file = luigi.Parameter()

    def requires(self):
        return [
            CreateLayer(
                self.prefix,
                self.config_file,
                PROCEDURE_SUBENTRIES_COLLECTION,
                "study",
                CreateProcedureSubentriesCollection(self.prefix, self.config_file),
            )
        ]

    def run(self):
        os.makedirs(luigi_targets_folder(self.prefix, self.config_file, self), exist_ok=True)
        with self.output().open("w"):
            pass

    def output(self):
        folder = luigi_targets_folder(self.prefix, self.config_file, self)
        return luigi.LocalTarget(
            os.path.join(
                folder, "create_study_layer_in_{}_collection_view_done".format(PROCEDURE_SUBENTRIES_COLLECTION)
            )
        )


class CreatePricecodeLayerInProcedureSubentries(luigi.Task):
    """

    """

    prefix = luigi.Parameter()
    config_file = luigi.Parameter()

    def requires(self):
        return [
            CreateLayer(
                self.prefix,
                self.config_file,
                PROCEDURE_SUBENTRIES_COLLECTION,
                "pricecode",
                CreateStudyLayerInProcedureSubentries(self.prefix, self.config_file),
            )
        ]

    def run(self):
        os.makedirs(luigi_targets_folder(self.prefix, self.config_file, self), exist_ok=True)
        with self.output().open("w"):
            pass

    def output(self):
        folder = luigi_targets_folder(self.prefix, self.config_file, self)
        return luigi.LocalTarget(
            os.path.join(
                folder, "create_pricecode_layer_in_{}_collection_view_done".format(PROCEDURE_SUBENTRIES_COLLECTION)
            )
        )


class CreateStudyTextLayerInProcedureSubentries(luigi.Task):
    """

    """

    prefix = luigi.Parameter()
    config_file = luigi.Parameter()

    def requires(self):
        return [
            CreateLayer(
                self.prefix,
                self.config_file,
                PROCEDURE_SUBENTRIES_COLLECTION,
                "study_text",
                CreatePricecodeLayerInProcedureSubentries(self.prefix, self.config_file),
            )
        ]

    def run(self):
        os.makedirs(luigi_targets_folder(self.prefix, self.config_file, self), exist_ok=True)
        with self.output().open("w"):
            pass

    def output(self):
        folder = luigi_targets_folder(self.prefix, self.config_file, self)
        return luigi.LocalTarget(
            os.path.join(
                folder, "create_study_text_layer_in_{}_collection_view_done".format(PROCEDURE_SUBENTRIES_COLLECTION)
            )
        )


class CreateDiagnosisLayerInProcedureSubentries(luigi.Task):
    """

    """

    prefix = luigi.Parameter()
    config_file = luigi.Parameter()

    def requires(self):
        return [
            CreateLayer(
                self.prefix,
                self.config_file,
                PROCEDURE_SUBENTRIES_COLLECTION,
                "diagnosis",
                CreateStudyTextLayerInProcedureSubentries(self.prefix, self.config_file),
            )
        ]

    def run(self):
        os.makedirs(luigi_targets_folder(self.prefix, self.config_file, self), exist_ok=True)
        with self.output().open("w"):
            pass

    def output(self):
        folder = luigi_targets_folder(self.prefix, self.config_file, self)
        return luigi.LocalTarget(
            os.path.join(
                folder, "create_diagnosis_layer_in_{}_collection_view_done".format(PROCEDURE_SUBENTRIES_COLLECTION)
            )
        )
