import luigi

from cda_data_cleaning.common.luigi_tasks import CDAJob, EmptyTask

from cda_data_cleaning.data_cleaning.analysis_data_cleaning.step00_create_cleaning_functions.create_cleaning_functions import CreateCleaningFunctions
from cda_data_cleaning.fact_extraction.diag_text_parsing.create_diag_parsing_layers import CreateDiagParsingLayers
from cda_data_cleaning.fact_extraction.diag_text_parsing.create_diag_text_collection import CreateDiagTextCollection
from cda_data_cleaning.fact_extraction.diag_text_parsing.create_extracted_diags_table import CreateExtractedDiagsTable
from cda_data_cleaning.fact_extraction.diag_text_parsing.export_tagger_mappings_from_csv import ExportTaggerMappingsFromCsv
from cda_data_cleaning.fact_extraction.diag_text_parsing.export_layers_to_table import ExportDiagTextLayersToTable
from cda_data_cleaning.fact_extraction.diag_text_parsing.delete_temp_tables import DeleteTempTables


class RunDiagTextParsing(CDAJob):
    prefix = luigi.Parameter(default="")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def requires(self):
        self.log_current_time("Starting time")

        task_00 = CreateCleaningFunctions(
            config_file=self.config_file,
            role=self.role,
            schema=self.work_schema,
            luigi_targets_folder=self.luigi_targets_folder,
        )

        task_01 = CreateDiagTextCollection(
            config_file=self.config_file,
            prefix=self.prefix,
            requirement=task_00
        )

        task_02 = CreateDiagParsingLayers(
            config_file=self.config_file,
            prefix=self.prefix,
            requirement=task_01
        )

        task_03 = ExportDiagTextLayersToTable(
            config_file=self.config_file,
            prefix=self.prefix,
            requirement=task_02
        )

        task_04 = ExportTaggerMappingsFromCsv(
            config_file=self.config_file,
            prefix=self.prefix,
            requirement=task_03
        )

        task_05 = CreateExtractedDiagsTable(
            config_file=self.config_file,
            prefix=self.prefix,
            requirement=task_04
        )

        task_06 = DeleteTempTables(
            config_file=self.config_file,
            prefix=self.prefix,
            requirement=task_05
        )

        return [
            task_00,
            task_01,
            task_02,
            task_03,
            task_04,
            task_05,
            task_06
        ]

    def run(self):
        self.log_current_time("Ending time")
        self.mark_as_complete()
