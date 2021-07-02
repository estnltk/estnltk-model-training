import luigi
from psycopg2 import sql

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import read_config
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import CDABatchTask
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import create_connection


class DeleteAllTables(CDABatchTask):
    """
    Deletes all tables that were created during analysis data cleaning pipeline
    """

    prefix = luigi.Parameter()
    config_file = luigi.Parameter()
    luigi_targets_folder = luigi.Parameter(default=".")

    def run(self):
        config = read_config(str(self.config_file))
        schema = config["database-configuration"]["work_schema"]
        role = config["database-configuration"]["role"]

        table_names = [
            "analysis_cleaned",
            "analysis_matched",
            "analysis_entry",
            "analysis_entry_cleaned",
            "analysis_entry_loinced",
            "analysis_entry_loinced_unique",
            "analysis_html",
            "analysis_html_log",
            "analysis_html_meta",
            "analysis_html_cleaned",
            "analysis_html_loinced",
            "analysis_html_loinced_unique",
            "analysis_parameter_name_to_loinc_mapping",
            "analysis_parameter_name_unit_to_loinc_mapping",
            "analysis_ties",
            "elabor_analysis_name_to_panel_loinc",
            "elabor_parameter_name_to_loinc_mapping",
            "elabor_parameter_name_unit_to_loinc_mapping",
            "long_loinc_mapping",
            "long_loinc_mapping_unique",
            "parameter_name_to_loinc_mapping",
            "parameter_name_unit_to_loinc_mapping",
            "parameter_unit_to_cleaned_unit",
            "parameter_unit_to_loinc_unit_mapping",
            "possible_units",
        ]

        conn = create_connection(config)
        for table in table_names:
            table_name = str(self.prefix) + table
            query = sql.SQL(
                """
                set role {role};
                drop table if exists  {schema}.{table_name};
            """
            ).format(role=sql.Identifier(role), schema=sql.Identifier(schema), table_name=sql.Identifier(table_name))

            cur = conn.cursor()
            cur.execute(query)
            conn.commit()

            cur.close()
        conn.close()

        self.mark_as_complete()
