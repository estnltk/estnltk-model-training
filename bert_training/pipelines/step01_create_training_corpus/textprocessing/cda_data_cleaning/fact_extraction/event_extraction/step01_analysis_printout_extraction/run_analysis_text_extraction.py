from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import CDAJob

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.data_cleaning.analysis_data_cleaning import (
    CreateCleaningFunctions,
)
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction import (
    CreateAnalysisPrintoutTables,
)
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction import (
    ExtractAnalysisPrintout,
)
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction.event_extraction.step01_analysis_printout_extraction.extract_analysis_structured_printout import (
    ExtractStructuredPrintout,
)
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction import (
    ImportPossibleUnitsTable,
)
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction import (
    CleanAnalysisPrintoutTable,
)
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction import (
    QualityOverviewOfResults,
)
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction import (
    CreatePrintoutLayer,
)


class RunAnalysisTextExtraction(CDAJob):
    """
    The aim is to extract analysis tables from `text` field in tables.
    Some analysis tables are wrongly placed under other tables (procedures, anamnesis etc)
    text field. This step extracts those tables from text then structures and cleans them.
    Finally, the results are added to texts collection as a layer printout segments.

    Requires for <prefix>_texts collection to exist.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table_prefix = str(self.prefix) + "_" if len(str(self.prefix)) > 0 else ""

    def requires(self):
        self.log_current_time("Starting time")

        task_00 = self.requirement

        task_01 = CreateCleaningFunctions(
            config_file=self.config_file,
            role=self.role,
            schema=self.work_schema,
            luigi_targets_folder=self.luigi_targets_folder,
            possible_units=self.table_prefix + "possible_units",
        )

        task_02 = CreateAnalysisPrintoutTables(
            config_file=self.config_file,
            role=self.role,
            target_schema=self.work_schema,
            target_table_texts=self.table_prefix + "analysis_texts",
            target_table_texts_struc=self.table_prefix + "analysis_texts_structured",
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_00,
        )

        task_03 = ExtractAnalysisPrintout(
            config_file=self.config_file,
            role=self.role,
            prefix=self.prefix,
            source_schema=self.original_schema,
            target_schema=self.work_schema,
            target_table=self.table_prefix + "analysis_texts",
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_02,
        )

        task_04 = ExtractStructuredPrintout(
            config_file=self.config_file,
            role=self.role,
            source_schema=self.work_schema,
            source_table=self.table_prefix + "analysis_texts",
            target_schema=self.work_schema,
            target_table=self.table_prefix + "analysis_texts_structured",
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_03,
        )

        # Possible units table needed for cleaning
        task_05 = ImportPossibleUnitsTable(
            config_file=self.config_file,
            role=self.role,
            schema=self.work_schema,
            target_table=self.table_prefix + "possible_units",
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_04,
        )

        task_06 = CleanAnalysisPrintoutTable(
            config_file=self.config_file,
            role=self.role,
            schema=self.work_schema,
            source_table=self.table_prefix + "analysis_texts_structured",
            target_table=self.table_prefix + "analysis_texts_structured_cleaned",
            possible_units=self.table_prefix + "possible_units",
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_05,
        )

        task_07 = QualityOverviewOfResults(
            config_file=self.config_file,
            schema=self.work_schema,
            sourcetable=self.table_prefix + "analysis_texts_structured_cleaned",
            output_filename=self.table_prefix + "quality_overview_analysis_texts_structured_cleaned",
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_06,
        )

        # create printout layer (printout_segments) to texts collection
        task_08 = CreatePrintoutLayer(
            prefix=self.table_prefix[:-1],  # prefix must not end with "_" in create layer
            config_file=self.config_file,
            role=self.role,
            schema=self.work_schema,
            collection="texts",
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_00,
        )

        return [task_00, task_01, task_02, task_03, task_04, task_05, task_06, task_07, task_08]

    def run(self):
        self.log_current_time("Ending time")
        self.mark_as_complete()
