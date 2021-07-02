import luigi

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import CDAJob

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.data_cleaning.analysis_data_cleaning import CreateCleaningFunctions
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction import CreateDiagParsingLayers
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction import CreateDiagTextCollection
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction import CreateExtractedDiagsTable
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction import ExportTaggerMappingsFromCsv
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction.diag_text_parsing.export_layers_to_table import ExportDiagTextLayersToTable
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction import DeleteTempTables


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
