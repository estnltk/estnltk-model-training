import luigi

from cda_data_cleaning.common.luigi_tasks import CDAJob, EmptyTask

from cda_data_cleaning.fact_extraction.text_extraction.create_texts_collection import CreateTextsCollection
from cda_data_cleaning.fact_extraction.event_extraction.step01_analysis_printout_extraction.run_analysis_text_extraction import RunAnalysisTextExtraction
from cda_data_cleaning.fact_extraction.event_extraction.step02_create_events_collection.event_extraction import CreateEventsCollection
from cda_data_cleaning.fact_extraction.measurement_extraction.run_extract_measurements_to_table import RunExtractMeasurementsToTable


class RunMasterWorkflow(CDAJob):
    prefix = luigi.Parameter(default="")

    def requires(self):
        self.log_current_time("Starting time")

        task_01 = CreateTextsCollection(
            config_file=self.config_file,
            prefix=self.prefix,
            requirement=self.requirement
        )

        task_02 = RunAnalysisTextExtraction(
            config_file=self.config_file,
            prefix=self.prefix,
            requirement=task_01
        )

        task_03 = CreateEventsCollection(
            prefix=self.prefix,
            config_file=self.config_file,
            requirement=task_02
        )

        task_04 = RunExtractMeasurementsToTable(
            config_file=self.config_file,
            prefix=self.prefix,
            requirement=task_03
        )

        return [
            task_01,
            task_02,
            task_03,
            task_04
        ]

    def run(self):
        self.log_current_time("Ending time")
        self.mark_as_complete()
