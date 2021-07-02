import luigi
import time
from psycopg2.sql import SQL, Identifier, Literal

from estnltk.layer_operations import split_by, extract_sections
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import CDAJob, CDASubtask
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction import CreateLayer
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction import (
    EventTokenTagger,
    EventHeaderTagger,
    EventSegmentsTagger,
    EventTokenConflictResolverTagger,
)

# export PYTHONPATH=~/Repos/cda-data-cleaning:$PYTHONPATH
# prefix="run_202102081533"
# conf="configurations/egcut_epi_microrun.ini"
# luigi --scheduler-port 8082 --module cda_data_cleaning.fact_extraction.event_extraction.step02_create_events_collection.create_events_collection CreateEventsCollection --prefix=$prefix --conf=$conf --workers=8


class CreateEventsCollection(CDAJob):
    """Tag events in the `<prefix>_texts` collection and create a separate `<prefix>_events` collection.

    """

    prefix = luigi.Parameter()
    config_file = luigi.Parameter()

    def requires(self):

        # check if 'texts' collection exists, if not then create it
        conn = self.create_postgres_connection(self.config_file)
        texts_table_exists = self.db_table_exists(
            conn=conn, table=str(self.prefix) + "_texts", schema=self.work_schema
        )
        printout_layer_exists = self.db_table_exists(
            conn=conn, table=str(self.prefix) + "_texts__printout_segments__layer", schema=self.work_schema
        )

        #  @TODO - checks in required field do not allow for master workflow to implement them

        # if not printout_layer_exists:
        #     # should run step01
        #     error = "Printout segments layer does not exist ({prefix}_texts__printout_segments__layer). Run step01_analysis_printout_extraction first!".format(
        #         prefix=self.prefix
        #     )
        #     raise Exception(error)
        #
        # if not texts_table_exists:
        #     # should be resolved:
        #     # problem here is task_02 starts running before task_01 is complete for some mysterious reason
        #     # task_01 = CreateTextsCollection2(prefix=self.prefix, config_file=self.config_file)
        #     error = "Tables {prefix}_texts and {prefix}_texts__structure do not exist, first run workflow in fact_extraction/text_extraction and then try again!".format(
        #         prefix=self.prefix
        #     )
        #     raise Exception(error)

        task_01 = self.requirement

        task_02 = CreateLayer(
            prefix=self.prefix,
            config_file=self.config_file,
            work_schema=self.work_schema,
            role=self.role,
            collection="texts",
            layer="event_tokens",
            tagger=EventTokenTagger(output_layer="event_tokens"),
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_01,
        )

        # event_tokens2 layer is a layer, that does not contain conflicting tokens with printouts
        task_03 = CreateLayer(
            prefix=self.prefix,
            config_file=self.config_file,
            work_schema=self.work_schema,
            role=self.role,
            collection="texts",
            layer="event_tokens2",
            tagger=EventTokenConflictResolverTagger(input_layers=["printout_segments", "event_tokens"]),
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_02,
        )

        task_04 = CreateLayer(
            prefix=self.prefix,
            config_file=self.config_file,
            work_schema=self.work_schema,
            role=self.role,
            collection="texts",
            layer="event_headers",
            tagger=EventHeaderTagger(output_layer="event_headers", layer_of_tokens="event_tokens2"),
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_03,
        )

        task_05 = CreateEmptyEventsCollection(self.prefix, self.config_file, requirement=task_04)

        # finally create event segments layer
        task_06 = CreateLayer(
            prefix=self.prefix,
            config_file=self.config_file,
            work_schema=self.work_schema,
            role=self.role,
            collection="texts",
            layer="event_segments",
            tagger=EventSegmentsTagger(),
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_05,
        )

        return [task_01, task_02, task_03, task_04, task_05, task_06]

    def run(self):
        self.log_current_action("CreateEventsCollection")
        self.log_schemas()

        storage = self.create_estnltk_storage(self.config_file)
        collection = storage[str(self.prefix) + "_texts"]

        events_collection = storage[str(self.prefix) + "_events"]
        events_collection.column_names = [
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

        with events_collection.insert() as insert:
            for id_, input_text, meta in collection.select(
                collection_meta=["epi_id", "epi_type", "schema", "table", "field", "row_id", "effective_time"],
                layers=["event_segments", "anonymised"],
            ):
                split_by(text=input_text, layer="event_segments", layers_to_keep=["anonymised"])
                sections = [(span.start, span.end) for span in input_text.event_segments]
                texts = extract_sections(text=input_text, sections=sections, layers_to_keep=["anonymised"])
                for text, event_segment_span in zip(texts, input_text.event_segments):
                    metadata = meta
                    metadata["texts_id"] = id_

                    metadata["header"] = event_segment_span.header
                    metadata["header_offset"] = event_segment_span.header_offset
                    metadata["event_header_date"] = event_segment_span.DATE
                    metadata["doctor_code"] = event_segment_span.DOCTOR_CODE
                    metadata["specialty"] = event_segment_span.SPECIALTY
                    metadata["specialty_code"] = event_segment_span.SPECIALTY_CODE

                    metadata["event_offset"] = event_segment_span.start

                    insert(text, meta_data=metadata)

        self.log_current_time("Ending time")
        self.mark_as_complete()


class CreateEmptyInputLayer(CDASubtask):
    """Create an empty table for a layer in the database.

    """

    prefix = luigi.Parameter()
    config_file = luigi.Parameter()
    schema = luigi.Parameter()
    role = luigi.Parameter()
    collection = luigi.Parameter()
    input_layer = luigi.Parameter()
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    def run(self):
        self.log_current_action("CreateEmptyInputLayer" + self.input_layer)
        self.log_schemas()

        storage = self.create_estnltk_storage(config=self.config_file, work_schema=self.schema, role=self.role)
        storage[str(self.prefix) + "_" + str(self.collection)].create_layer_table(
            layer_name=self.input_layer, meta=None
        )

        self.mark_as_complete()


class CreateEmptyEventsCollection(CDASubtask):
    """Create an empty EstNLTK collection `<prefix>_events` for the events.

    """

    prefix = luigi.Parameter()
    config_file = luigi.Parameter()
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    def run(self):
        self.log_current_action("CreateEmptyEventsCollection")
        self.log_schemas()

        storage = self.create_estnltk_storage(self.config_file)

        collection = storage[str(self.prefix) + "_events"]
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

        self.mark_as_complete()


# TODO not in use right now
class CreateEventsRelease(CDASubtask):
    """Creates a view of `<prefix>_events` collection with raw text instead of json data.

    """

    prefix = luigi.Parameter()
    config_file = luigi.Parameter()
    luigi_targets_folder = luigi.Parameter(default=".")

    def requires(self):
        return [
            CreateEmptyEventsCollection(self.prefix, self.config_file, luigi_targets_folder=self.luigi_targets_folder)
        ]

    def run(self):
        self.log_current_action("CreateEventsRelease")
        self.log_schemas()

        schema = self.work_schema
        view = SQL("{}.{}").format(schema, Identifier(str(self.prefix) + "_extracted_events"))
        events_table = SQL("{}.{}").format(schema, Identifier(str(self.prefix) + "_events"))

        storage = self.create_estnltk_storage(self.config_file)

        query = SQL(
            "CREATE VIEW {view} AS "
            "SELECT "
            "id, "
            "data->>'text' AS raw_text, "
            "epi_id, "
            "epi_type, "
            "schema, "
            '"table", '
            '"field", '
            "row_id, "
            # -- In *_drugs we have effective_time as text, in a format like '2013-12-13 09:53:18';
            # https://git.stacc.ee/project4/egcut-anonym-cda/blob/master/releases/r007_preparations.sql#L368-369
            "to_char(effective_time, 'YYYY-MM-DD HH24:MI:SS') AS effective_time, "
            "header, "
            "header_offset, "
            "event_offset "
            "FROM {events_table}; "
            "COMMENT ON VIEW {view} IS {comment};"
        ).format(
            view=view,
            events_table=events_table,
            comment=Literal(
                "created by {} on {} by running {} task".format(storage.user, time.asctime(), self.__class__.__name__)
            ),
        )

        with storage.conn.cursor() as cursor:
            cursor.execute(query)

        self.mark_as_complete()
