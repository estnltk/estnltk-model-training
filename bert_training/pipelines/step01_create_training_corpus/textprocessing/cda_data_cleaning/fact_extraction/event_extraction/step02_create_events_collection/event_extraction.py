import luigi

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import CDAJob
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction.text_extraction import CreateTextsCollection
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction import (
    CreateEventsCollection,
)


class ExtractEvents(CDAJob):
    """Runs CreateTextsCollection and CreateEventsCollection workflows.

    prefix: str
        any string used as a collection name prefix and luigi_targets folder name
    config_file: str
        name of the configuration ini file in the cda-data_cleaning/configurations folder

    Creates EstNLTK collection table for events <schema>.<table> where
    <schema> = <database-configuration.work_schema> deterimined in configuration file and
    <table> = <prefix>_events

    """

    prefix = luigi.Parameter()
    config_file = luigi.Parameter()

    def requires(self):
        self.log_current_time("Starting time")

        task_00 = self.requirement
        task_01 = CreateTextsCollection(prefix=self.prefix, config_file=self.config_file, requirement=task_00)
        task_02 = CreateEventsCollection(prefix=self.prefix, config_file=self.config_file, requirement=task_01)

        return [task_00, task_01, task_02]

    def run(self):
        self.log_current_time("Ending time")
        self.mark_as_complete()
