import luigi

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import CDAJob

# @TODO Currently developing
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.data_cleaning.analysis_data_cleaning import CreateCleaningFunctions
from .step00_create_raw_tables.create_ambulatory_case import CreateAmbulatoryCaseTable
from .step00_create_raw_tables.create_patient import CreatePatientTable
from .step00_create_raw_tables.create_department_stay import CreateDepartmentStayTable
from .step01_create_epi_times.create_epi_times_table import CreateEpiTimesTable
from .step01_create_epi_times.fill_epi_times_table import FillEpiTimesTable
from .step02_clean_epi_times.clean_epi_times_table import CleanEpiTimesTable
from .step03_delete_tables.delete_raw_tables import DeleteRawEpiTables


class RunEpicrisisDateCleaning(CDAJob):
    prefix = luigi.Parameter(default="")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table_prefix = str(self.prefix) + "_" if len(str(self.prefix)) > 0 else ""

    def requires(self):
        self.log_current_time("Starting time")

        # Step 01: Create PSQL cleaning functions (taken from RunAnalysisDataCleaning)
        task_01 = CreateCleaningFunctions(
            config_file=self.config_file,
            role=self.role,
            schema=self.work_schema,
            luigi_targets_folder=self.luigi_targets_folder,
        )

        # Step 02: Create and fill Ambulatory Case table
        task_02 = CreateAmbulatoryCaseTable(
            config_file=self.config_file,
            role=self.role,
            source_schema=self.original_schema,
            source_table="ambulatory_case",
            target_schema=self.work_schema,
            target_table=self.table_prefix + "ambulatory_case_raw",
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_01
        )

        # Step 03: Create and fill Patient table
        task_03 = CreatePatientTable(
            config_file=self.config_file,
            role=self.role,
            source_schema=self.original_schema,
            source_table="patient",
            target_schema=self.work_schema,
            target_table=self.table_prefix + "patient_raw",
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_02
        )

        # Step 04: Create and fill Department Stay table
        task_04 = CreateDepartmentStayTable(
            config_file=self.config_file,
            role=self.role,
            source_schema=self.original_schema,
            source_table="department_stay",
            target_schema=self.work_schema,
            target_table=self.table_prefix + "department_stay_raw",
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_03
        )

        # Step 05: Create epi_times table
        task_05 = CreateEpiTimesTable(
            config_file=self.config_file,
            role=self.role,
            target_schema=self.work_schema,
            target_table=self.table_prefix + "epi_times",
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_04
        )

        # Step 06: Fill epi_times table
        task_06 = FillEpiTimesTable(
            config_file=self.config_file,
            prefix=self.prefix,
            target_schema=self.work_schema,
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_05
        )

        # Step 07: Clean epi_times table
        task_07 = CleanEpiTimesTable(
            config_file=self.config_file,
            target_schema=self.work_schema,
            target_table=self.table_prefix + "epi_times",
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_06
        )

        # Step 08: Delete raw patient, department_stay and ambulatory_case tables
        task_08 = DeleteRawEpiTables(
            config_file=self.config_file,
            role=self.role,
            prefix=self.prefix,
            target_schema=self.work_schema,
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_07
        )

        return [
            task_01,
            task_02,
            task_03,
            task_04,
            task_05,
            task_06,
            task_07,
            task_08
        ]

    def run(self):
        self.log_current_time("Ending time")
        self.mark_as_complete()
