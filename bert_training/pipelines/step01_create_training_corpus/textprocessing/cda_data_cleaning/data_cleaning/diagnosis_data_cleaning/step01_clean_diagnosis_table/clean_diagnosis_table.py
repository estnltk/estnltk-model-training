import luigi

from cda_data_cleaning.common.luigi_tasks import CDASubtask
from cda_data_cleaning.common.db_operations import create_database_connection_string
import cda_data_cleaning.data_cleaning.diagnosis_data_cleaning.step01_clean_diagnosis_table.clean_values as clean


class CleanDiagnosisTable(CDASubtask):

    config_file = luigi.Parameter()
    target_schema = luigi.Parameter(default="work")
    target_table = luigi.Parameter(default="diagnosis")
    luigi_targets_folder = luigi.Parameter(default=".")
    prefix = luigi.Parameter(default="")

    def run(self):
        self.log_current_action("Clean Diagnosis table")

        # Creates the database connection string required for SQL Alchemy operations
        config = self.read_config(self.config_file)
        db_string = create_database_connection_string(config['database-configuration'])

        clean.main(
            prefix=self.prefix,
            work_schema=self.target_schema,
            target_table=self.target_table,
            database_connection_string=db_string
        )

        self.mark_as_complete()
