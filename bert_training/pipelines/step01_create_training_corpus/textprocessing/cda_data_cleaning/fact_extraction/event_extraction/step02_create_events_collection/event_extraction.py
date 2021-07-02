import luigi

from cda_data_cleaning.common.luigi_tasks import CDAJob, EmptyTask
from cda_data_cleaning.fact_extraction.text_extraction.create_texts_collection import CreateTextsCollection
from cda_data_cleaning.fact_extraction.event_extraction.step02_create_events_collection.create_events_collection import (
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
