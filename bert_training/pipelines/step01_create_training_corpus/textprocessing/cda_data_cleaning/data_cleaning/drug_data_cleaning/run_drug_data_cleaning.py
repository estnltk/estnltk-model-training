import os
import luigi

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import CDAJob

from .step00_create_cleaning_functions.create_cleaning_functions import CreateCleaningFunctions
from .step00_create_cleaning_functions.validate_assumptions import ValidateAssumptions
from .step01_clean_entry.clean_entry import CleanEntry
from .step01_clean_entry.clean_entry import LogCleaningResults
from .step02_parse_and_clean_drug_text_field.parse_drug_text_field import ParseDrugTextField
from .step02_parse_and_clean_drug_text_field.clean_drug_parsed import CleanParsed
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.data_cleaning.drug_data_cleaning import (
    ImportMappingTable,
)
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.data_cleaning.drug_data_cleaning import (
    PerformMapping,
)
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.data_cleaning.drug_data_cleaning import (
    LogMissingDataInParsed,
)

from .step03_match_entry_parsed.match_entry_and_parsed import MatchEntryParsed
from .step04_final_table.create_final_table import CreateATCtoOMOP
from .step04_final_table.create_final_table import CreateFinalTable
from .step04_final_table.validate_results import ValidateResults


class RunDrugDataCleaning(CDAJob):
    """
    Executes the whole drug cleaning pipeline.
    Output table is named 'prefix' + '_drug_cleaned'.
    """

    prefix = luigi.Parameter(default="")
    config_file = luigi.Parameter()
    output_file = luigi.Parameter(default="")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table_prefix = str(self.prefix) + "_" if len(str(self.prefix)) > 0 else ""

        # TODO: This is not a good location for a log file. Find a better default
        if len(self.output_file) == 0:
            self.output_file = os.path.dirname(__file__) + "/" + str(self.prefix) + "log.txt"

        # By default Python does not work with Unix paths like ~/dir
        # Use magic lines to convert path into absolute path. This avoids WTF errors
        self.output_file = os.path.abspath(os.path.expanduser(os.path.expandvars(self.output_file)))

    def requires(self):
        self.log_current_time("Starting time")

        # Step00: Validate assumptions
        task_000 = ValidateAssumptions(
            config_file=self.config_file, role=self.role, luigi_targets_folder=self.luigi_targets_folder,
        )

        # Step00: Create cleaning functions
        task_00 = CreateCleaningFunctions(
            config_file=self.config_file,
            role=self.role,
            schema=self.work_schema,
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_000,
        )

        # Step01: Parse drug data
        task_01 = ParseDrugTextField(
            config_file=self.config_file,
            role=self.role,
            source_schema=self.original_schema,
            source_table="drug",
            target_schema=self.work_schema,
            target_table=self.table_prefix + "drug_parsed",
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_00,
        )

        # Step01: Clean parsed drug data
        task_02 = CleanParsed(
            config_file=self.config_file,
            role=self.role,
            schema=self.work_schema,
            source_table=self.table_prefix + "drug_parsed",
            target_table=self.table_prefix + "drug_parsed_cleaned",
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_01,
        )

        # Step02: Clean structured drug data
        task_03 = CleanEntry(
            config_file=self.config_file,
            role=self.role,
            source_schema=self.original_schema,
            source_table="drug_entry",  # here should NOT be original.<prefix>_drug_entry, instead original.drug_entry
            target_schema=self.work_schema,
            target_table=self.table_prefix + "drug_entry_cleaned",
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_02,
        )

        task_04 = LogCleaningResults(
            config_file=self.config_file,
            role=self.role,
            schema=self.work_schema,
            entry_source_table=self.table_prefix + "drug_entry_cleaned",
            parsed_source_table=self.table_prefix + "drug_parsed_cleaned",
            output_file=self.output_file,
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_03,
        )

        # Step02.5: Map missing data in parsed
        task_05 = ImportMappingTable(
            config_file=self.config_file,
            role=self.role,
            target_schema=self.work_schema,
            target_table=self.table_prefix + "drug_packages",
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_04,
        )

        task_06 = PerformMapping(
            config_file=self.config_file,
            role=self.role,
            schema=self.work_schema,
            table=self.table_prefix + "drug_parsed_cleaned",
            mapping_schema=self.mapping_schema,
            mapping_table=self.table_prefix + "drug_packages",
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_05,
        )

        task_07 = LogMissingDataInParsed(
            config_file=self.config_file,
            role=self.role,
            schema=self.work_schema,
            source_table=self.table_prefix + "drug_parsed_cleaned",
            output_file=self.output_file,
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_06,
        )

        # Step 03: Match data form text and entry tables
        task_08 = MatchEntryParsed(
            config_file=self.config_file,
            role=self.role,
            schema=self.mapping_schema,
            text_source_table=self.table_prefix + "drug_parsed_cleaned",
            entry_source_table=self.table_prefix + "drug_entry_cleaned",
            matched_table=self.table_prefix + "drug_matched",
            matched_wo_duplicates_table=self.table_prefix + "drug_matched_wo_duplicates",
            table_prefix=self.table_prefix,
            output_file=self.output_file,
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_07,
        )

        # Step 04: Create final table and do OMOP mapping
        task_09 = CreateATCtoOMOP(
            config_file=self.config_file,
            role=self.role,
            target_schema=self.mapping_schema,
            target_table=self.table_prefix + "atc_to_omop",
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_08,
        )

        task_10 = CreateFinalTable(
            config_file=self.config_file,
            role=self.role,
            schema=self.work_schema,
            source_table=self.table_prefix + "drug_matched_wo_duplicates",
            target_table=self.table_prefix + "drug_cleaned",
            mapping_schema=self.mapping_schema,
            mapping_table=self.table_prefix + "atc_to_omop",
            output_file=self.output_file,
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_09,
        )

        task_11 = ValidateResults(
            config_file=self.config_file,
            schema=self.work_schema,
            source_table=self.table_prefix + "drug_cleaned",
            output_file=str(self.prefix) + "validation_output",
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_10,
        )

        return [
            task_000,
            task_00,
            task_01,
            task_02,
            task_03,
            task_04,
            task_05,
            task_06,
            task_07,
            task_08,
            task_09,
            task_10,
            task_11,
        ]

    def run(self):
        self.log_current_time("Ending time")
        self.mark_as_complete()
