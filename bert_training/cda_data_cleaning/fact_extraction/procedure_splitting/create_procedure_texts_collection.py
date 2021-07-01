import luigi
import os
import time
import psycopg2
from psycopg2.sql import SQL, Identifier, Literal
from cda_data_cleaning.common.luigi_tasks import CDAJob, CDASubtask, EmptyTask

from estnltk import Text

from cda_data_cleaning.common import etsa_date_string_to_date

# Kui AnonymisedTagger-it on siin vaja, siis võiks ta kuhugi commonisse tõsta.
from cda_data_cleaning.fact_extraction.event_extraction.step02_create_events_collection.taggers import AnonymisedTagger

# vaata sinna faili sisse ja otsusta milliste tabelite millised veerud on vajalikud
from .procedure_texts_extraction_conf import EXTRACTABLE_TABLE_FIELDS, map_table_to_effective_time_field


PROCEDURE_TEXTS_COLLECTION = "procedure_texts"


class CreateProcedureTextsCollection(CDAJob):
    """Extract procedure text fields from the source tables and create an EstNLTK <prefix>_<PROCEDURE_TEXTS_COLLECTION>
    collection.

    """

    prefix = luigi.Parameter()
    config_file = luigi.Parameter()

    source_table_list = [i for i, v in EXTRACTABLE_TABLE_FIELDS["egcut_epi"].items()]
    fields_list = [v for i, v in EXTRACTABLE_TABLE_FIELDS["egcut_epi"].items()]

    def requires(self):
        # config = self.read_config(self.config_file)

        task_00 = EmptyTask()

        task_01 = CreateEmptyCollection(self.prefix, self.config_file, requirement=task_00)

        task_02 = CreateCollectionView(self.prefix, self.config_file, requirement=task_01)

        task_03 = ImportFieldsToCollection(
            prefix=self.prefix,
            config_file=self.config_file,
            schema=self.work_schema,
            original_schema=self.original_schema,
            source_table_list=self.source_table_list,
            fields_list=self.fields_list,
            requirement=task_02
        )

        return [task_00, task_01, task_02, task_03]

    def run(self):
        self.mark_as_complete()


class CreateEmptyCollection(CDASubtask):
    """Create an empty EstNLTK collection.

    """

    prefix = luigi.Parameter()
    config_file = luigi.Parameter()
    requirement = luigi.TaskParameter()

    def requires(self):
        return [self.requirement]

    def run(self):
        storage = self.create_estnltk_storage(self.config_file)

        collection = storage[str(self.prefix) + "_" + PROCEDURE_TEXTS_COLLECTION]
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


class ImportFieldsToCollection(CDASubtask):
    """Import text fields and meta from source tables to an EstNLTK collection.

    """

    prefix = luigi.Parameter()
    config_file = luigi.Parameter()
    schema = luigi.Parameter()
    original_schema = luigi.Parameter()
    source_table_list = luigi.ListParameter()
    fields_list = luigi.ListParameter()
    requirement = luigi.TaskParameter()

    def requires(self):
        return [self.requirement]

    def run(self):

        assert len(self.fields_list) == len(self.source_table_list)

        anonymised_tagger = AnonymisedTagger()

        input_conn = self.create_postgres_connection(self.config_file)

        storage = self.create_estnltk_storage(self.config_file)

        for i in range(len(self.fields_list)):
            fields = self.fields_list[i]
            table = self.source_table_list[i]

            collection = storage[str(self.prefix) + "_" + PROCEDURE_TEXTS_COLLECTION]
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


class CreateCollectionView(CDASubtask):
    """Creates a view of a collection with raw text instead of json data.

    """

    prefix = luigi.Parameter()
    config_file = luigi.Parameter()
    requirement = luigi.TaskParameter()

    def requires(self):
        return [self.requirement]

    def run(self):
        config = self.read_config(self.config_file)

        schema = Identifier(config["database-configuration"]["work_schema"])
        view = SQL("{}.{}").format(schema, Identifier(str(self.prefix) + "_extracted_" + PROCEDURE_TEXTS_COLLECTION))
        texts_table = SQL("{}.{}").format(schema, Identifier(str(self.prefix) + "_" + PROCEDURE_TEXTS_COLLECTION))

        storage = self.create_estnltk_storage(self.config_file)
        query = SQL(
            "CREATE VIEW {view} AS "
            "SELECT "
            "id, "
            """data->>'text' AS "raw_text", """
            "epi_id, "
            "epi_type, "
            "schema, "
            '"table", '
            '"field", '
            "row_id, "
            "effective_time::text "
            "FROM {texts_table}; "
            "COMMENT ON VIEW {view} IS {comment};"
        ).format(
            view=view,
            texts_table=texts_table,
            comment=Literal(
                "created by {} on {} by running {} task".format(
                    storage.user, time.asctime(), self.__class__.__name__
                )
            ),
        )

        with storage.conn.cursor() as cursor:
            cursor.execute(query)

        storage.close()

        self.mark_as_complete()
