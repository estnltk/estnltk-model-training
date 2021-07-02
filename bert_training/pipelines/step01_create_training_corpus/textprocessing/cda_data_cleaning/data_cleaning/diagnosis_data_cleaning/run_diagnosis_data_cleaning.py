import luigi

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import CDAJob

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import CreateDiagnosisTable
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import FillDiagnosisTable
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import CleanDiagnosisTable


class RunDiagnosisDataCleaning(CDAJob):
    prefix = luigi.Parameter(default="")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table_prefix = str(self.prefix) + "_" if len(str(self.prefix)) > 0 else ""

    def requires(self):
        self.log_current_time("Starting time")

        task_00 = CreateDiagnosisTable(
            config_file=self.config_file,
            role=self.role,
            target_schema=self.work_schema,
            target_table=self.table_prefix + "diagnosis",
            luigi_targets_folder=self.luigi_targets_folder
        )

        task_01 = FillDiagnosisTable(
            config_file=self.config_file,
            source_schema=self.original_schema,
            target_schema=self.work_schema,
            target_table=self.table_prefix + "diagnosis",
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_00
        )

        task_02 = CleanDiagnosisTable(
            config_file=self.config_file,
            target_schema=self.work_schema,
            target_table=self.table_prefix + 'diagnosis',
            luigi_targets_folder=self.luigi_targets_folder,
            prefix=self.prefix,
            requirement=task_01
        )

        return [
            task_00,
            task_01,
            task_02
        ]

    def run(self):
        self.log_current_time("Ending time")
        self.mark_as_complete()
