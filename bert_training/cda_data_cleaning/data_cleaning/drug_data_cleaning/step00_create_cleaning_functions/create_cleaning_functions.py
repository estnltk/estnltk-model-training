import os
import luigi
from psycopg2 import sql

from cda_data_cleaning.common.luigi_tasks import CDATask, CDASubtask, CDABatchTask


class CreateCleaningFunctions(CDATask):
    """
    Creates cleaning postgresql functions for original.drug_entry column 'drug_code_display_name'
    """

    config_file = luigi.Parameter()
    role = luigi.Parameter()
    schema = luigi.Parameter(default="work")
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    def requires(self):
        self.log_current_action("Create cleaning functions")

        task_01 = CreateDoseQuantityValueCleaningFunctions(
            config_file=self.config_file,
            role=self.role,
            schema=self.schema,
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=self.requirement,
        )
        task_02 = CreateDrugCodeDisplayNameCleaningFunctions(
            config_file=self.config_file,
            role=self.role,
            schema=self.schema,
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=self.requirement,
        )

        return [task_01, task_02]

    def run(self):
        self.mark_as_complete()


class CreateDoseQuantityValueCleaningFunctions(CDASubtask):
    config_file = luigi.Parameter()
    role = luigi.Parameter()
    schema = luigi.Parameter(default="work")
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    def run(self):
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cleaning_file = os.path.join(os.path.dirname(__file__), "psql_cleaning_functions/dose_quantity_value.psql")

        with open(cleaning_file) as sql_string:
            cur.execute(
                sql.SQL(sql_string.read()).format(schema=sql.Identifier(self.schema), role=sql.Literal(self.role))
            )
            conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()


class CreateDrugCodeDisplayNameCleaningFunctions(CDASubtask):
    config_file = luigi.Parameter()
    role = luigi.Parameter()
    schema = luigi.Parameter(default="work")
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    def run(self):
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cleaning_file = os.path.join(
            os.path.dirname(__file__), "psql_cleaning_functions/drug_code_display_name_function.psql"
        )

        with open(cleaning_file) as sql_string:
            cur.execute(
                sql.SQL(sql_string.read()).format(schema=sql.Identifier(self.schema), role=sql.Literal(self.role))
            )
            conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()
