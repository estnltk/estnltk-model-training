import luigi
import os
import time
from psycopg2.sql import SQL, Identifier, Literal

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.shared_workflows import ApplyLoincMapping
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import luigi_targets_folder
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import CDASubtask

# manually extracted regex_types of objective findings
objective_finding_regex_types = (
    "APGAR",
    "KAAL",
    "KAALULANGUS",
    "KAALUTÕUS",
    "KMI",
    "LUUTIHEDUS",
    "PIKKUS",
    "PULSS",
    "VERERÕHK",
    "VÖÖÜMBERMÕÕT",
)


class ApplyLoincMappingToMeasurements(luigi.Task):
    prefix = luigi.Parameter()
    config_file = luigi.Parameter()

    def requires(self):
        export_measurements_to_table = ExportMeasurementsToTable(self.prefix, self.config_file)
        return [
            export_measurements_to_table,
            ApplyLoincMapping(
                self.prefix,
                self.config_file,
                "_extracted_measurements",
                "_extracted_measurements_loinced",
                export_measurements_to_table,
            ),
        ]

    def run(self):
        os.makedirs(luigi_targets_folder(self.prefix, self.config_file, self), exist_ok=True)
        with self.output().open("w"):
            pass

    def output(self):
        folder = luigi_targets_folder(self.prefix, self.config_file, self)
        return luigi.LocalTarget(os.path.join(folder, "apply_loinc_mapping_to_measurements_done"))


class ExportMeasurementsToTable(CDASubtask):
    prefix: str = luigi.Parameter()
    config_file = luigi.Parameter()
    schema = luigi.Parameter()
    role = luigi.Parameter()

    def requires(self):
        return [self.requirement]

    def run(self):
        measurements_table = SQL("{}.{}").format(
            Identifier(self.schema), Identifier(self.prefix + "_extracted_measurements")
        )

        storage = self.create_estnltk_storage(self.config_file)

        # with get_postgres_storage(self.config_file) as storage:
        collection = storage[self.prefix + "_events"]

        collection.export_layer(
            layer="measurements",
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
                "event_header_date",
                "doctor_code",
                "specialty",
                "specialty_code",
            ],
            table_name=self.prefix + "_extracted_measurements",
        )

        query = SQL(
            "ALTER TABLE {measurements_table} "
            "ADD COLUMN parameter_name text, "
            "ADD COLUMN parameter_name_raw text, "
            "ADD COLUMN parameter_unit_raw text; "
            # trim whitespaces;
            # is TRIM enough for OBJECT?
            "UPDATE {measurements_table} "
            "SET "
            'parameter_name = lower(TRIM("OBJECT")), '
            'parameter_name_raw = "OBJECT", '
            'parameter_unit_raw = "UNIT", '
            r"event_header_date = REGEXP_REPLACE(event_header_date, '^(\u00a0|\s)*(.*)(\u00a0|\s)*$', '\2');"
        ).format(measurements_table=measurements_table)

        with storage.conn.cursor() as cursor:
            cursor.execute(query)
        storage.conn.commit()

        storage.close()

        self.mark_as_complete()


class CreateObjectiveFindingTable(luigi.Task):
    prefix: str = luigi.Parameter()
    config_file = luigi.Parameter()

    def requires(self):
        return [ApplyLoincMappingToMeasurements(self.prefix, self.config_file)]

    def run(self):
        objective_finding_table = SQL("{}.{}").format(
            Identifier("work"), Identifier(self.prefix + "_objective_finding")
        )
        measurements_table = SQL("{}.{}").format(
            Identifier("work"), Identifier(self.prefix + "_extracted_measurements_loinced")
        )

        storage = self.create_estnltk_storage(self.config_file)

        query = SQL(
            "CREATE TABLE {table} "
            "AS SELECT * FROM {measurements_table} "
            'WHERE {measurements_table}."REGEX_TYPE" IN {objective_findings};'
        ).format(
            table=objective_finding_table,
            measurements_table=measurements_table,
            objective_findings=Literal(objective_finding_regex_types),
        )

        with storage.conn.cursor() as cursor:
            cursor.execute(query)
        storage.conn.commit()

        storage.close()

        os.makedirs(luigi_targets_folder(self.prefix, self.config_file, self), exist_ok=True)
        with self.output().open("w"):
            pass

    def output(self):
        folder = luigi_targets_folder(self.prefix, self.config_file, self)
        return luigi.LocalTarget(os.path.join(folder, "create_objective_finding_table_done"))


class CreateFreetextAnalysisTable(luigi.Task):
    prefix: str = luigi.Parameter()
    config_file = luigi.Parameter()

    def requires(self):
        return [ApplyLoincMappingToMeasurements(self.prefix, self.config_file)]

    def run(self):
        freetext_analysis_loinced_table = SQL("{}.{}").format(
            Identifier("work"), Identifier(self.prefix + "_freetext_analysis")
        )
        measurements_table = SQL("{}.{}").format(
            Identifier("work"), Identifier(self.prefix + "_extracted_measurements_loinced")
        )

        storage = self.create_estnltk_storage(self.config_file)

        query = SQL(
            "CREATE TABLE {table} "
            "AS SELECT * FROM {measurements_table} "
            'WHERE {measurements_table}."REGEX_TYPE" NOT IN {objective_findings};'
        ).format(
            table=freetext_analysis_loinced_table,
            measurements_table=measurements_table,
            objective_findings=Literal(objective_finding_regex_types),
        )

        with storage.conn.cursor() as cursor:
            cursor.execute(query)
        storage.conn.commit()

        storage.close()

        os.makedirs(luigi_targets_folder(self.prefix, self.config_file, self), exist_ok=True)
        with self.output().open("w"):
            pass

    def output(self):
        folder = luigi_targets_folder(self.prefix, self.config_file, self)
        return luigi.LocalTarget(os.path.join(folder, "create_freetext_analysis_table_done"))


# deprecated but seems to be much quicker than the new loincing workflow
class AddLoincParametersToFreetextAnalysisTable(luigi.Task):
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
        return [CreateFreetextAnalysisTable(self.prefix, self.config_file)]

    def run(self):
        export_table = SQL("{}.{}").format(Identifier("work"), Identifier(self.prefix + "_freetext_analysis_loinced"))

        storage = self.create_estnltk_storage(self.config_file)
        query = SQL(
            "ALTER TABLE {export_table} "
            "ADD COLUMN loinc_code text, "
            "ADD COLUMN t_lyhend text; "
            "UPDATE {export_table} AS m "
            "SET loinc_code = r.loinc_code, t_lyhend = r.t_lyhend "
            "FROM analysiscleaning.regextype_to_loinc AS r "
            'WHERE m."REGEX_TYPE" = r.regex_type; '
            "UPDATE {export_table} AS m "
            "SET loinc_code = e.loinc_code, t_lyhend = e.t_lyhend "
            "FROM analysiscleaning.run_201905271226elabor_parameter_name_to_loinc_mapping AS e "
            "WHERE m.parameter_name = lower(e.parameter_name); "
            "UPDATE {export_table} AS m "
            "SET loinc_code = p.loinc_code, t_lyhend = p.t_lyhend "
            "FROM analysiscleaning.run_201905271226parameter_name_to_loinc_mapping AS p "
            "WHERE m.parameter_name = lower(p.parameter_name); "
            "UPDATE {export_table} AS m "
            "SET loinc_code = e.loinc_code, t_lyhend = e.t_lyhend "
            "FROM analysiscleaning.run_201905271226elabor_parameter_name_parameter_unit_to_loinc_mapping AS e "
            'WHERE m.parameter_name = lower(e.parameter_name) and m."UNIT" = e.t_yhik; '
            "UPDATE {export_table} AS m "
            "SET loinc_code = p.loinc_code, t_lyhend = p.t_lyhend "
            "FROM analysiscleaning.run_201905271226parameter_name_parameter_unit_to_loinc_mapping AS p "
            'WHERE m.parameter_name = lower(p.parameter_name) and m."UNIT" = p.parameter_unit; '
        ).format(export_table=export_table)

        with storage.conn.cursor() as cursor:
            cursor.execute(query)
        storage.conn.commit()

        storage.close()

        os.makedirs(luigi_targets_folder(self.prefix, self.config_file, self), exist_ok=True)
        with self.output().open("w"):
            pass

    def output(self):
        folder = luigi_targets_folder(self.prefix, self.config_file, self)
        return luigi.LocalTarget(os.path.join(folder, "add_loinc_parameters_to_freetex_analysis_table_done"))


class CreateFreetextAnalysisViewWithRawText(luigi.Task):
    prefix: str = luigi.Parameter()
    config_file = luigi.Parameter()

    def requires(self):
        return [CreateFreetextAnalysisTable(self.prefix, self.config_file)]

    def run(self):
        schema = Identifier("work")
        view = SQL("{}.{}").format(schema, Identifier(self.prefix + "_freetext_analysis_with_text"))
        texts_table = SQL("{}.{}").format(schema, Identifier(self.prefix + "_texts"))
        events_table = SQL("{}.{}").format(schema, Identifier(self.prefix + "_events"))
        export_table = SQL("{}.{}").format(schema, Identifier(self.prefix + "_freetext_analysis"))

        storage = self.create_estnltk_storage(self.config_file)

        query = SQL(
            "CREATE VIEW {view} AS "
            "SELECT {export_table}.text_id, "
            "{export_table}.span_nr, "
            "{export_table}.span_start, "
            "{export_table}.span_end, "
            "{export_table}.name, "
            "{export_table}.parameter_name, "
            '{export_table}."VALUE" AS value, '
            '{export_table}."UNIT" AS unit, '
            '{export_table}."MIN" AS min, '
            '{export_table}."MAX" AS max, '
            '{export_table}."DATE" AS date, '
            '{export_table}."REGEX_TYPE" AS regex_type, '
            "{export_table}.loinc_code, "
            "{export_table}.elabor_t_lyhend, "
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
            "{export_table}.event_header_date, "
            "{export_table}.doctor_code, "
            "{export_table}.specialty, "
            "{export_table}.specialty_code, "
            "{texts_table}.data->>'text' AS field_text, "
            "{events_table}.data->>'text' AS event_text "
            "FROM {export_table}, {texts_table}, {events_table} "
            "WHERE {texts_table}.id = {events_table}.texts_id "
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

        storage.close()

        os.makedirs(luigi_targets_folder(self.prefix, self.config_file, self), exist_ok=True)
        with self.output().open("w"):
            pass

    def output(self):
        folder = luigi_targets_folder(self.prefix, self.config_file, self)
        return luigi.LocalTarget(os.path.join(folder, "create_freetext_analysis_view_with_raw_text_done"))


class CreateObjectiveFindingViewWithRawText(luigi.Task):
    prefix: str = luigi.Parameter()
    config_file = luigi.Parameter()

    def requires(self):
        return [CreateObjectiveFindingTable(self.prefix, self.config_file)]

    def run(self):
        schema = Identifier("work")
        view = SQL("{}.{}").format(schema, Identifier(self.prefix + "_objective_finding_with_text"))
        texts_table = SQL("{}.{}").format(schema, Identifier(self.prefix + "_texts"))
        events_table = SQL("{}.{}").format(schema, Identifier(self.prefix + "_events"))
        export_table = SQL("{}.{}").format(schema, Identifier(self.prefix + "_objective_finding"))

        storage = self.create_estnltk_storage(self.config_file)

        query = SQL(
            "CREATE VIEW {view} AS "
            "SELECT {export_table}.text_id, "
            "{export_table}.span_nr, "
            "{export_table}.span_start, "
            "{export_table}.span_end, "
            "{export_table}.name, "
            "{export_table}.parameter_name, "
            '{export_table}."VALUE" AS value, '
            '{export_table}."UNIT" AS unit, '
            '{export_table}."MIN" AS min, '
            '{export_table}."MAX" AS max, '
            '{export_table}."DATE" AS date, '
            '{export_table}."REGEX_TYPE" AS regex_type, '
            # '{export_table}.loinc_code, '
            # '{export_table}.t_lyhend, '
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
            "{export_table}.event_header_date, "
            "{export_table}.doctor_code, "
            "{export_table}.specialty, "
            "{export_table}.specialty_code, "
            "{texts_table}.data->>'text' AS field_text, "
            "{events_table}.data->>'text' AS event_text "
            "FROM {export_table}, {texts_table}, {events_table} "
            "WHERE {texts_table}.id = {events_table}.texts_id "
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

        storage.close()

        os.makedirs(luigi_targets_folder(self.prefix, self.config_file, self), exist_ok=True)
        with self.output().open("w"):
            pass

    def output(self):
        folder = luigi_targets_folder(self.prefix, self.config_file, self)
        return luigi.LocalTarget(os.path.join(folder, "create_objective_finding_view_with_raw_text_done"))


class CreateMeasurementViews(luigi.Task):
    prefix = luigi.Parameter()
    config_file = luigi.Parameter()

    def requires(self):
        return [
            CreateObjectiveFindingViewWithRawText(self.prefix, self.config_file),
            CreateFreetextAnalysisViewWithRawText(self.prefix, self.config_file),
        ]

    def run(self):
        os.makedirs(luigi_targets_folder(self.prefix, self.config_file, self), exist_ok=True)
        with self.output().open("w"):
            pass

    def output(self):
        folder = luigi_targets_folder(self.prefix, self.config_file, self)
        return luigi.LocalTarget(os.path.join(folder, "create_measureent_views_done"))
