import luigi

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import CDAJob
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.shared_workflows import ApplyLoincMapping

from .step00_create_cleaning_functions.create_cleaning_functions import CreateCleaningFunctions
from .step01_create_and_clean_analysis_entry_table.create_analysis_entry import CreateAnalysisEntryTable
from .step01_create_and_clean_analysis_entry_table.clean_analysis_entry import CleanAnalysisEntryTable
from .step01_create_and_clean_analysis_entry_table.create_possible_units_table import CreatePossibleUnitsTable
from .step02_create_and_clean_analysis_html_table.create_analysis_html_table import CreateAnalysisHtmlTable
from .step02_create_and_clean_analysis_html_table.clean_analysis_html_table import CleanAnalysisHtmlTable
from .step02_create_and_clean_analysis_html_table.parse_analysis_html_table import ParseAnalysisHtmlTable
from .step02_create_and_clean_analysis_html_table.add_units_to_possible_units_table import AddUnitsToPossibleUnitsTable
from .step03_delete_duplicates.delete_duplicates import DeleteDuplicates
from .step04_match_entry_and_html.create_final_table import CreateFinalTable
from .step04_match_entry_and_html.match_entry_and_html import MatchEntryHtml
from .step05_summarise.summarise_analysis_cleaned import SummariseAnalysisCleaned


class RunAnalysisDataCleaning(CDAJob):
    prefix = luigi.Parameter(default="")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table_prefix = str(self.prefix) + "_" if len(str(self.prefix)) > 0 else ""

    def requires(self):
        self.log_current_time("Starting time")

        # Step 00:  Create cleaning functions
        task_01 = CreateCleaningFunctions(
            config_file=self.config_file,
            role=self.role,
            schema=self.work_schema,
            luigi_targets_folder=self.luigi_targets_folder,
            possible_units=self.table_prefix + "possible_units",
        )

        # Step 01: Create and clean entry table
        task_02 = CreateAnalysisEntryTable(
            config_file=self.config_file,
            role=self.role,
            source_schema=self.original_schema,
            source_table="analysis_entry",
            target_schema=self.work_schema,
            target_table=self.table_prefix + "analysis_entry",
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_01,
        )

        # Possible units table contains all distinct parameter_unit_raw-s from analysis_entry table
        # In the cleaning step it is used to clean column value_raw,
        # because in some cases value_raw contains both value and unit
        task_03 = CreatePossibleUnitsTable(
            config_file=self.config_file,
            role=self.role,
            schema=self.work_schema,
            source_table=self.table_prefix + "analysis_entry",
            target_table=self.table_prefix + "possible_units",
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_02,
        )

        task_04 = CleanAnalysisEntryTable(
            config_file=self.config_file,
            role=self.role,
            schema=self.work_schema,
            source_table=self.table_prefix + "analysis_entry",
            target_table=self.table_prefix + "analysis_entry_cleaned",
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_03,
        )

        # Step 02: Create and clean analysis html table
        task_05 = CreateAnalysisHtmlTable(
            config_file=self.config_file,
            role=self.role,
            target_schema=self.work_schema,
            target_table=self.table_prefix + "analysis_html",
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_04,
        )

        task_06 = ParseAnalysisHtmlTable(
            config_file=self.config_file,
            role=self.role,
            source_schema=self.original_schema,
            source_table="analysis",
            target_schema=self.work_schema,
            target_table=self.table_prefix + "analysis_html",
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_05,
        )

        # All distinct parameter_unit_raw-s from analysis_html table are added to the possible_units table
        # In the cleaning step it is used to clean column value_raw,
        # because in some cases value_raw contains both value and unit
        task_07 = AddUnitsToPossibleUnitsTable(
            config_file=self.config_file,
            role=self.role,
            schema=self.work_schema,
            source_table=self.table_prefix + "analysis_html",
            target_table=self.table_prefix + "possible_units",
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_06,
        )

        task_08 = CleanAnalysisHtmlTable(
            config_file=self.config_file,
            role=self.role,
            schema=self.work_schema,
            source_table=self.table_prefix + "analysis_html",
            target_table=self.table_prefix + "analysis_html_cleaned",
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_07,
        )

        # Step 03: LOINC-ing and duplicates
        task_09 = ApplyLoincMapping(
            config_file=self.config_file,
            role=self.role,
            schema=self.work_schema,
            source_table=self.table_prefix + "analysis_html_cleaned",
            target_table=self.table_prefix + "analysis_html_loinced",
            mapping_schema=self.mapping_schema,
            mapping_prefix=self.prefix,
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_08,
        )

        task_10 = ApplyLoincMapping(
            config_file=self.config_file,
            role=self.role,
            schema=self.work_schema,
            source_table=self.table_prefix + "analysis_entry_cleaned",
            target_table=self.table_prefix + "analysis_entry_loinced",
            mapping_schema=self.mapping_schema,
            mapping_prefix=self.prefix,
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_09,
        )

        task_11 = DeleteDuplicates(
            config_file=self.config_file,
            role=self.role,
            schema=self.work_schema,
            html_source_table=self.table_prefix + "analysis_html_loinced",
            entry_source_table=self.table_prefix + "analysis_entry_loinced",
            html_target_table=self.table_prefix + "analysis_html_loinced_unique",
            entry_target_table=self.table_prefix + "analysis_entry_loinced_unique",
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_10,
        )

        # Step 04: Matching entry and html tables
        task_12 = MatchEntryHtml(
            config_file=self.config_file,
            role=self.role,
            schema=self.work_schema,
            html_source_table=self.table_prefix + "analysis_html_loinced_unique",
            entry_source_table=self.table_prefix + "analysis_entry_loinced_unique",
            matched_table=self.table_prefix + "analysis_matched",
            ties_table=self.table_prefix + "analysis_ties",
            table_prefix=self.table_prefix,
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_11,
        )

        task_13 = CreateFinalTable(
            config_file=self.config_file,
            schema=self.work_schema,
            role=self.role,
            source_table=self.table_prefix + "analysis_matched",
            target_table=self.table_prefix + "analysis_cleaned",
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_12,
        )

        task_14 = SummariseAnalysisCleaned(
            config_file=self.config_file,
            schema=self.work_schema,
            role=self.role,
            source_table=self.table_prefix + "analysis_cleaned",
            temp_table=self.table_prefix + "analysis_cleaned_summary_counts",
            target_table=self.table_prefix + "analysis_cleaned_summary",
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_13,
        )

        return [
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
            task_12,
            task_13,
            task_14,
        ]

    def run(self):
        self.log_current_time("Ending time")
        self.mark_as_complete()
