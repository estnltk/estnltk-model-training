import luigi
import time
from psycopg2.sql import SQL, Identifier, Literal
from estnltk import Text

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import CDATask, CDASubtask, CDAJob, \
    EmptyTask
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import etsa_date_string_to_date

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction.event_extraction.step02_create_events_collection.taggers.anonymised_tagger import (
    AnonymisedTagger,
)
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction.text_extraction import (
    EXTRACTABLE_TABLE_FIELDS,
    map_table_to_effective_time_field,
)


# export PYTHONPATH=~/Repos/cda-data-cleaning:$PYTHONPATH
# prefix="run_202101251135"
# conf="configurations/egcut_epi_microrun.ini"
# luigi --scheduler-port 8082 --module cda_data_cleaning.fact_extraction.text_extraction.create_texts_collection CreateTextsCollection2 --prefix=$prefix --conf=$conf --workers=8


class CreateTextsCollection(CDAJob):
    """Extract text fields from <source> tables and create EstNLTK texts collection.

    TODO: 1. Convert it to CDAJob
          2. Introduce a configuration option for defining extractable fields
          3. Remove map of maps nonsense for extractable fields
    """

    prefix = luigi.Parameter(default="")
    config_file = luigi.Parameter()
    source_schema = luigi.Parameter(default="original")
    target_schema = luigi.Parameter(default="work")
    target_collection = luigi.Parameter(default="texts")

    # def __init__(self, *args, **kwargs):
    #    super().__init__(*args, **kwargs)

    def requires(self):

        task_00 = self.requirement

        task_01 = CreateEmptyTextsCollection(
            prefix=self.prefix,
            config_file=self.config_file,
            work_schema=self.work_schema,
            role=self.role,
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_00
        )

        task_02 = CreateTextsRelease(
            prefix=self.prefix,
            config_file=self.config_file,
            work_schema=self.work_schema,
            role=self.role,
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_01,
        )

        source_table_list = [i for i, v in EXTRACTABLE_TABLE_FIELDS["egcut_epi"].items()]
        fields_list = [v for i, v in EXTRACTABLE_TABLE_FIELDS["egcut_epi"].items()]

        task_03 = ImportFieldsToTextsCollection(
                    prefix=self.prefix,
                    config_file=self.config_file,
                    schema=self.work_schema,  # config["database-configuration"]["work_schema"],
                    original_schema=self.original_schema,  # config["database-configuration"]["original_schema"],
                    table_list=source_table_list,
                    fields_list=fields_list,
                    luigi_targets_folder=self.luigi_targets_folder,
                    requirement=task_02
                )

        return [task_00, task_01, task_02, task_03]

    def run(self):
        print("\n\nCreate texts collections is done!\n\n")
        self.mark_as_complete()


class CreateEmptyTextsCollection(CDATask):
    """
    Create empty EstNLTK texts collection.
    TODO: Make parameters explicit
          Document collection as markdown file
          Define CDAFactExtractionTask not reasonable ?
    """

    prefix = luigi.Parameter()
    config_file = luigi.Parameter()
    work_schema = luigi.Parameter(default="work")
    role = luigi.Parameter()
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter(default=EmptyTask())

    def requires(self):
        return [self.requirement]

    def run(self):
        self.log_current_action("CreateEmptyTextsCollection")
        self.log_schemas()

        storage = self.create_estnltk_storage(config=self.config_file, work_schema=self.work_schema, role=self.role)
        collection = storage[str(self.prefix) + "_texts"]
        collection.create(
            meta={
                "epi_id": "str",
                "epi_type": "str",
                "schema": "str",
                "table": "str",
                "field": "str",
                "row_id": "str",
                "effective_time": "datetime",
            }
        )

        storage.close()
        self.mark_as_complete()


class CreateTextsRelease(CDASubtask):
    """
    Creates a view of texts collection with raw text instead of json data.
    TODO: Document resulting table as md

    """

    prefix = luigi.Parameter()
    config_file = luigi.Parameter()
    work_schema = luigi.Parameter(default="work")
    role = luigi.Parameter()
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    def requires(self):
        return [self.requirement]

    def run(self):
        self.log_current_action("CreateTextsRelease")
        self.log_schemas()

        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        # Move up to top tasks
        view = SQL("{}.{}").format(Identifier(self.work_schema), Identifier(str(self.prefix) + "_extracted_texts"))
        texts_table = SQL("{}.{}").format(Identifier(self.work_schema), Identifier(str(self.prefix) + "_texts"))

        comment = "created by {} on {} by running {} task".format(self.role, time.asctime(), self.__class__.__name__)

        cur.execute(
            SQL(
                """
            CREATE VIEW {view} AS 
            SELECT 
                id, 
                data->>'text' AS raw_text,
                epi_id, 
                epi_type, 
                schema, 
                "table", 
                field, 
                row_id, 
                effective_time::text 
            FROM {texts_table}; 
            COMMENT ON VIEW {view} IS {comment};
            """
            ).format(view=view, texts_table=texts_table, comment=Literal(comment))
        )

        cur.close()
        conn.close()
        self.mark_as_complete()


class ImportFieldsToTextsCollection(CDASubtask):
    """Import text fields and meta from source tables to EstNLTK texts collection.
    """

    prefix = luigi.Parameter()
    config_file = luigi.Parameter()
    schema = luigi.Parameter()
    original_schema = luigi.Parameter()
    table_list = luigi.ListParameter()
    fields_list = luigi.ListParameter()
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    def requires(self):
        return [self.requirement]

    def run(self):
        self.log_current_action(
            "ImportFieldsToTextsCollection, table:" + str(self.table_list) + ", fields:" + str(self.fields_list)
        )
        self.log_schemas()

        anonymised_tagger = AnonymisedTagger()

        input_conn = self.create_postgres_connection(self.config_file)
        storage = self.create_estnltk_storage(self.config_file)

        collection = storage[str(self.prefix) + "_texts"]
        collection.column_names = [
            "id",
            "data",
            "epi_id",
            "epi_type",
            "schema",
            "table",
            "field",
            "row_id",
            "effective_time",
        ]

        if len(self.fields_list) != len(self.table_list):
            raise ValueError("Length of fields list and source table list isn't the same")

        for i in range(len(self.fields_list)):
            fields = self.fields_list[i]
            table = self.table_list[i]

            query = "SELECT id, epi_id, epi_type, {} AS effective_time, {} FROM {}.{}".format(
                map_table_to_effective_time_field.get(table, "NULL"),
                ", ".join(fields),
                self.original_schema,
                table,
            )

            storage.conn.commit()
            storage.conn.autocommit = False
            with input_conn.cursor("read", withhold=True) as cursor, collection.insert() as insert:
                cursor.execute(query)
                for row in cursor:
                    id_, epi_id, epi_type, effective_time, *texts = row
                    effective_time = etsa_date_string_to_date(effective_time, suppress_exceptions=False)
                    for t, field in zip(texts, fields):
                        if t is None:
                            continue
                        text = Text(t)
                        anonymised_tagger.tag(text)
                        insert(
                            text,
                            meta_data={
                                "epi_id": epi_id,
                                "epi_type": epi_type,
                                "schema": self.original_schema,
                                "table": table,
                                "field": field,
                                "row_id": id_,
                                "effective_time": effective_time,
                            },
                        )
        input_conn.close()
        storage.close()

        self.mark_as_complete()
