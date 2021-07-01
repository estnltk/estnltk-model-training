import luigi
import time
from psycopg2.sql import SQL, Identifier, Literal
from estnltk import Text

from cda_data_cleaning.common.luigi_tasks import CDATask, CDASubtask, CDAJob, CDABatchTask, EmptyTask
from cda_data_cleaning.fact_extraction.event_extraction.step02_create_events_collection.taggers.anonymised_tagger import (
    AnonymisedTagger,
)

# export PYTHONPATH=~/Repos/cda-data-cleaning:$PYTHONPATH
# prefix="run_202101251135"
# conf="configurations/egcut_epi_microrun.ini"
# luigi --scheduler-port 8082 --module cda_data_cleaning.fact_extraction.diag_text_parsing.create_diag_text_collection CreateDiagTextCollection --prefix=$prefix --conf=$conf --workers=1


class CreateDiagTextCollection(CDAJob):
    prefix = luigi.Parameter(default="")
    config_file = luigi.Parameter()
    source_schema = luigi.Parameter(default="original")
    target_schema = luigi.Parameter(default="work")
    target_collection = luigi.Parameter(default="diagnosis")
    luigi_targets_folder = luigi.Parameter(default="luigi_targets")

    # def __init__(self, *args, **kwargs):
    #    super().__init__(*args, **kwargs)

    def requires(self):
        task_00 = self.requirement

        task_01 = CreateEmptyDiagTextCollection(
            prefix=self.prefix,
            config_file=self.config_file,
            work_schema=self.work_schema,
            role=self.role,
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_00
        )

        task_02 = CreateDiagTextRelease(
            prefix=self.prefix,
            config_file=self.config_file,
            work_schema=self.work_schema,
            role=self.role,
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_01,
        )

        task_03 = ImportFieldsToDiagTextsCollection(
            prefix=self.prefix,
            config_file=self.config_file,
            schema=self.work_schema,  # config["database-configuration"]["work_schema"],
            original_schema=self.original_schema,  # config["database-configuration"]["original_schema"],
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_02
        )

        return [task_00, task_01, task_02, task_03]

    def run(self):
        print("\n\nCreate texts collections is done!\n\n")
        self.mark_as_complete()


class CreateEmptyDiagTextCollection(CDATask):
    prefix = luigi.Parameter()
    config_file = luigi.Parameter()
    work_schema = luigi.Parameter(default="work")
    role = luigi.Parameter()
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter(default=EmptyTask())

    def requires(self):
        return [self.requirement]

    def run(self):
        self.log_current_action("CreateEmptyDiagTextCollection")
        self.log_schemas()

        storage = self.create_estnltk_storage(config=self.config_file, work_schema=self.work_schema, role=self.role)
        collection = storage[str(self.prefix) + "_diagnosis_parsing"]
        collection.create(
            meta={
                "src_id": "str",
                "epi_id": "str",
                "field": "str",
                "table": "str",
                "collection": "str"

            }
        )

        storage.close()
        self.mark_as_complete()


class CreateDiagTextRelease(CDASubtask):

    prefix = luigi.Parameter()
    config_file = luigi.Parameter()
    work_schema = luigi.Parameter(default="work")
    role = luigi.Parameter()
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    def requires(self):
        return [self.requirement]

    def run(self):
        self.log_current_action("CreateDiagTextRelease")
        self.log_schemas()

        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        # Move up to top tasks
        view = SQL("{}.{}").format(Identifier(self.work_schema), Identifier(str(self.prefix) + "_extracted_diag_texts"))
        texts_table = SQL("{}.{}").format(Identifier(self.work_schema),
                                          Identifier(str(self.prefix) + "_diagnosis_parsing"))

        comment = "created by {} on {} by running {} task".format(self.role, time.asctime(), self.__class__.__name__)

        cur.execute(
            SQL(
                """
            CREATE VIEW {view} AS 
            SELECT 
                id,
                src_id,
                epi_id,
                data->>'diag_text' AS raw_text
            FROM {texts_table}; 
            COMMENT ON VIEW {view} IS {comment};
            """
            ).format(view=view, texts_table=texts_table, comment=Literal(comment))
        )

        cur.close()
        conn.close()
        self.mark_as_complete()


class ImportFieldsToDiagTextsCollection(CDASubtask):
    """Import text fields and meta from source tables to EstNLTK texts collection.
    """

    prefix = luigi.Parameter()
    config_file = luigi.Parameter()
    schema = luigi.Parameter()
    original_schema = luigi.Parameter()
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    def requires(self):
        return [self.requirement]

    def run(self):
        self.log_schemas()

        anonymised_tagger = AnonymisedTagger()

        input_conn = self.create_postgres_connection(self.config_file)
        storage = self.create_estnltk_storage(self.config_file)

        collection = storage[str(self.prefix) + "_diagnosis_parsing"]
        collection.column_names = [
            "id",
            "data",
            "src_id",
            "epi_id",
            "field",
            "table",
            "collection"
        ]

        table = str(self.prefix) + "_diagnosis"

        query = """
        SELECT 
            id, 
            src_id, 
            epi_id, 
            diag_text_raw,
            diag_name_additional
        FROM {}.{}""".format(
            self.schema,
            table,
        )

        storage.conn.commit()
        storage.conn.autocommit = False
        with input_conn.cursor("read", withhold=True) as cursor, collection.insert() as insert:
            cursor.execute(query)
            for row in cursor:
                id_, src_id, epi_id, diag_text_raw, diag_name_additional = row
                if diag_text_raw is not None:
                    text = Text(diag_text_raw)
                    anonymised_tagger.tag(text)
                    insert(
                        text,
                        meta_data={
                            "src_id": src_id,
                            "epi_id": epi_id,
                            "field": "diag_text_raw",
                            "table": table,
                            "collection": str(self.prefix) + "_diagnosis_parsing"
                        },
                    )

                if diag_name_additional is not None:
                    text = Text(diag_name_additional)
                    anonymised_tagger.tag(text)
                    insert(
                        text,
                        meta_data={
                            "src_id": src_id,
                            "epi_id": epi_id,
                            "field": "diag_name_additional",
                            "table": table,
                            "collection": str(self.prefix) + "_diagnosis_parsing"
                        },
                    )
        input_conn.close()
        storage.close()

        self.mark_as_complete()
