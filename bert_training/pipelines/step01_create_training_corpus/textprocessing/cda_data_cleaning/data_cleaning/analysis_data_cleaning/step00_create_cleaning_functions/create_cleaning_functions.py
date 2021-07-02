import os
import luigi
from psycopg2 import sql

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import CDATask
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import CDABatchTask
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.common.luigi_tasks.evaluate_sql_script import EvaluateSQLScript


class CreateCleaningFunctions(CDABatchTask):
    """
    Creates cleaning postgresql functions for columns
        - 'analysis_name_raw'
        - 'parameter_name_raw'
        - 'effective_time_raw'
        - 'reference_values_raw'
        - 'value_raw'
    """

    config_file = luigi.Parameter()
    role = luigi.Parameter()
    schema = luigi.Parameter(default="work")
    possible_units = luigi.Parameter(default="possible_units")
    luigi_targets_folder = luigi.Parameter(default=".")

    def requires(self):
        self.log_current_action("Create cleaning functions")
        self.log_schemas()
        task_01 = CreateEffectiveTimeCleaningFunctions(
            self.config_file, self.role, self.schema, self.luigi_targets_folder
        )
        task_02 = CreateValueCleaningFunctions(
            self.config_file, self.role, self.schema, self.possible_units, self.luigi_targets_folder
        )
        task_03 = CreateReferenceValueCleaningFunctions(
            self.config_file, self.role, self.schema, self.luigi_targets_folder
        )
        task_04 = CreateParameterNameCleaningFunctions(
            self.config_file, self.role, self.schema, self.luigi_targets_folder
        )
        task_05 = CreateAnalysisNameCleaningFunctions(
            self.config_file, self.role, self.schema, self.luigi_targets_folder
        )
        return [task_01, task_02, task_03, task_04, task_05]


class CreateAnalysisNameCleaningFunctions(CDATask):
    config_file = luigi.Parameter()
    role = luigi.Parameter()
    schema = luigi.Parameter(default="work")
    luigi_targets_folder = luigi.Parameter(default=".")

    def requires(self):

        analysis_cleaning_file = os.path.join(
            os.path.dirname(__file__), "psql_cleaning_functions/analysis_name_function.psql"
        )
        return [
            EvaluateSQLScript(
                config_file=self.config_file,
                role=self.role,
                schema=self.schema,
                psql_file=analysis_cleaning_file,
                luigi_targets_folder=self.luigi_targets_folder,
            )
        ]

    def run(self):
        self.mark_as_complete()


class CreateParameterNameCleaningFunctions(CDATask):
    config_file = luigi.Parameter()
    role = luigi.Parameter()
    schema = luigi.Parameter(default="work")
    luigi_targets_folder = luigi.Parameter(default=".")

    def requires(self):

        parameter_cleaning_file = os.path.join(
            os.path.dirname(__file__), "psql_cleaning_functions/parameter_name_function.psql"
        )
        return [
            EvaluateSQLScript(
                config_file=self.config_file,
                role=self.role,
                schema=self.schema,
                psql_file=parameter_cleaning_file,
                luigi_targets_folder=self.luigi_targets_folder,
            )
        ]

    def run(self):
        self.mark_as_complete()


class CreateEffectiveTimeCleaningFunctions(CDATask):
    config_file = luigi.Parameter()
    role = luigi.Parameter()
    schema = luigi.Parameter(default="work")
    luigi_targets_folder = luigi.Parameter(default=".")

    def requires(self):

        effective_time_cleaning_file = os.path.join(
            os.path.dirname(__file__), "psql_cleaning_functions/effective_time_function.psql"
        )
        return [
            EvaluateSQLScript(
                config_file=self.config_file,
                role=self.role,
                schema=self.schema,
                psql_file=effective_time_cleaning_file,
                luigi_targets_folder=self.luigi_targets_folder,
            )
        ]

    def run(self):
        self.mark_as_complete()


class CreateValueCleaningFunctions(CDATask):
    config_file = luigi.Parameter()
    role = luigi.Parameter()
    schema = luigi.Parameter(default="work")
    possible_units = luigi.Parameter(default="possible_units")
    luigi_targets_folder = luigi.Parameter(default=".")

    def requires(self):
        return []

    def run(self):
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        path = os.path.join(os.path.dirname(__file__), "psql_cleaning_functions/value_cleaning_functions")
        value_cleaning_files = [
            os.path.join(path, "float_functions.psql"),
            os.path.join(path, "integer_functions.psql"),
            os.path.join(path, "range_functions.psql"),
            os.path.join(path, "ratio_functions.psql"),
            os.path.join(path, "num_and_par_functions.psql"),
            os.path.join(path, "text_functions.psql"),
            os.path.join(path, "text_and_value_functions.psql"),
            os.path.join(path, "time_series_functions.psql"),
            os.path.join(path, "clean_value_functions.psql"),
        ]

        # TODO: figure out how to use evaluate_sql_script with extra argument possible units
        for file in value_cleaning_files:
            with open(file) as sql_string:
                cur.execute(
                    sql.SQL(sql_string.read()).format(
                        schema=sql.Identifier(self.schema),
                        role=sql.Literal(self.role),
                        possible_units=sql.Identifier(self.possible_units),
                    )
                )
                conn.commit()
        conn.close()
        self.mark_as_complete()


class CreateReferenceValueCleaningFunctions(CDATask):
    config_file = luigi.Parameter()
    role = luigi.Parameter()
    schema = luigi.Parameter(default="work")
    luigi_targets_folder = luigi.Parameter(default=".")

    def requires(self):
        reference_value_cleaning_file = os.path.join(
            os.path.dirname(__file__), "psql_cleaning_functions/reference_value_function.psql"
        )
        return [
            EvaluateSQLScript(
                config_file=self.config_file,
                role=self.role,
                schema=self.schema,
                psql_file=reference_value_cleaning_file,
                luigi_targets_folder=self.luigi_targets_folder,
            )
        ]

    def run(self):
        self.mark_as_complete()
