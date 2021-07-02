import luigi
import os
import time
from psycopg2.sql import SQL, Identifier, Literal

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import get_postgres_storage
from . import luigi_targets_folder
from .tag_measurements_dev import TagMeasurementsDev


class ExportMeasurementsToTable(luigi.Task):

    prefix: str = luigi.Parameter()
    config_file = luigi.Parameter()

    def requires(self):
        return [TagMeasurementsDev(self.prefix, self.config_file)]

    def run(self):
        with get_postgres_storage(self.config_file) as storage:
            collection = storage[self.prefix + "_events"]

            collection.export_layer(
                layer="measurements_dev",
                attributes=["name", "OBJECT", "VALUE", "UNIT", "MIN", "MAX", "DATE", "REGEX_TYPE"],
                collection_meta=[
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
                ],
                table_name=self.prefix + "_extracted_measurements_dev",
            )

        os.makedirs(luigi_targets_folder(self.prefix), exist_ok=True)
        with self.output().open("w"):
            pass

    def output(self):
        folder = luigi_targets_folder(self.prefix)
        return luigi.LocalTarget(os.path.join(folder, "export_measurements_to_table_done"))


class AddLoincParametersToMeasurementsTable(luigi.Task):
    """Adds `object`, `loinc_code` and `t_lyhend` columns to the measurements table.

    Using `object`, and `UNIT` values the measurements table is updated by joining with
    analysiscleaning.elabor_parameter_name_to_loinc_mapping
    analysiscleaning.parameter_name_to_loinc_mapping
    analysiscleaning.elabor_parameter_name_parameter_unit_to_loinc_mapping
    analysiscleaning.parameter_name_parameter_unit_to_loinc_mapping

    """

    prefix: str = luigi.Parameter()
    config_file = luigi.Parameter()

    def requires(self):
        return [ExportMeasurementsToTable(self.prefix, self.config_file)]

    def run(self):
        export_table = SQL("{}.{}").format(Identifier("work"), Identifier(self.prefix + "_extracted_measurements_dev"))

        with get_postgres_storage(self.config_file) as storage:

            query = SQL(
                "ALTER TABLE {export_table} "
                "ADD COLUMN object text, "
                "ADD COLUMN loinc_code text, "
                "ADD COLUMN t_lyhend text; "
                "UPDATE {export_table} AS m "
                'SET object = lower(TRIM(m."OBJECT")); '
                "UPDATE {export_table} AS m "
                "SET loinc_code = e.loinc_code, t_lyhend = e.t_lyhend "
                "FROM analysiscleaning.run_201905271226elabor_parameter_name_to_loinc_mapping AS e "
                "WHERE m.object = lower(e.parameter_name); "
                "UPDATE {export_table} AS m "
                "SET loinc_code = p.loinc_code, t_lyhend = p.t_lyhend "
                "FROM analysiscleaning.run_201905271226parameter_name_to_loinc_mapping AS p "
                "WHERE m.object = lower(p.parameter_name); "
                "UPDATE {export_table} AS m "
                "SET loinc_code = e.loinc_code, t_lyhend = e.t_lyhend "
                "FROM analysiscleaning.run_201905271226elabor_parameter_name_parameter_unit_to_loinc_mapping AS e "
                'WHERE m.object = lower(e.parameter_name) and m."UNIT" = e.t_yhik; '
                "UPDATE {export_table} AS m "
                "SET loinc_code = p.loinc_code, t_lyhend = p.t_lyhend "
                "FROM analysiscleaning.run_201905271226parameter_name_parameter_unit_to_loinc_mapping AS p "
                'WHERE m.object = lower(p.parameter_name) and m."UNIT" = p.parameter_unit; '
            ).format(export_table=export_table)

            with storage.conn.cursor() as cursor:
                cursor.execute(query)
            storage.conn.commit()

        os.makedirs(luigi_targets_folder(self.prefix), exist_ok=True)
        with self.output().open("w"):
            pass

    def output(self):
        folder = luigi_targets_folder(self.prefix)
        return luigi.LocalTarget(os.path.join(folder, "add_loinc_parameters_to_measurements_table_done"))


class CreateMeasurementsViewWithRawText(luigi.Task):

    prefix: str = luigi.Parameter()
    config_file = luigi.Parameter()

    def requires(self):
        return [AddLoincParametersToMeasurementsTable(self.prefix, self.config_file)]

    def run(self):
        schema = Identifier("work")
        view = SQL("{}.{}").format(schema, Identifier(self.prefix + "_extracted_measurements_with_text_dev"))
        texts_table = SQL("{}.{}").format(schema, Identifier(self.prefix + "_texts"))
        events_table = SQL("{}.{}").format(schema, Identifier(self.prefix + "_events"))
        export_table = SQL("{}.{}").format(schema, Identifier(self.prefix + "_extracted_measurements_dev"))

        with get_postgres_storage(self.config_file) as storage:
            query = SQL(
                "CREATE VIEW {view} AS "
                "SELECT {export_table}.text_id, "
                "{export_table}.span_nr, "
                "{export_table}.span_start, "
                "{export_table}.span_end, "
                "{export_table}.name, "
                '{export_table}."OBJECT" AS object, '
                '{export_table}."VALUE" AS value, '
                '{export_table}."UNIT" AS unit, '
                '{export_table}."MIN" AS min, '
                '{export_table}."MAX" AS max, '
                '{export_table}."DATE" AS date, '
                '{export_table}."REGEX_TYPE" AS regex_type, '
                "{export_table}.loinc_code, "
                "{export_table}.t_lyhend, "
                "{export_table}.epi_id, "
                "{export_table}.epi_type, "
                "{export_table}.schema, "
                '{export_table}."table", '
                '{export_table}."field", '
                "{export_table}.row_id, "
                "{export_table}.effective_time, "
                "{export_table}.header, "
                "{export_table}.header_offset, "
                "{export_table}.event_offset, "
                "{texts_table}.data->>'text' AS field_text, "
                "{events_table}.data->>'text' AS event_text "
                "FROM {export_table}, {texts_table}, {events_table} "
                "WHERE {texts_table}.epi_id = {events_table}.epi_id "
                "AND {texts_table}.epi_type = {events_table}.epi_type "
                "AND {texts_table}.schema = {events_table}.schema "
                "AND {texts_table}.table = {events_table}.table "
                "AND {texts_table}.field = {events_table}.field "
                "AND {texts_table}.row_id = {events_table}.row_id "
                "AND {events_table}.id = {export_table}.text_id;"
                "COMMENT ON VIEW {view} IS {comment};"
            ).format(
                view=view,
                texts_table=texts_table,
                export_table=export_table,
                events_table=events_table,
                comment=Literal(
                    "created by {} on {} by running {} task".format(
                        storage.user, time.asctime(), self.__class__.__name__
                    )
                ),
            )

            with storage.conn.cursor() as cursor:
                cursor.execute(query)

        os.makedirs(luigi_targets_folder(self.prefix), exist_ok=True)
        with self.output().open("w"):
            pass

    def output(self):
        folder = luigi_targets_folder(self.prefix)
        return luigi.LocalTarget(os.path.join(folder, "create_measurements_view_with_raw_text_done"))
