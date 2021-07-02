import luigi

from cda_data_cleaning.common.luigi_tasks import CDAJob

from cda_data_cleaning.data_cleaning.diagnosis_data_cleaning.step00_create_diagnosis_table.create_diagnosis_table import CreateDiagnosisTable
from cda_data_cleaning.data_cleaning.diagnosis_data_cleaning.step00_create_diagnosis_table.fill_diagnosis_table import FillDiagnosisTable
from cda_data_cleaning.data_cleaning.diagnosis_data_cleaning.step01_clean_diagnosis_table.clean_diagnosis_table import CleanDiagnosisTable


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
