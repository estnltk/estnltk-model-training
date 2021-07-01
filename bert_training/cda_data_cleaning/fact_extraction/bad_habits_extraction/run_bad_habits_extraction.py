import luigi

from cda_data_cleaning.common.luigi_tasks import CDAJob, CDASubtask, EmptyTask
from cda_data_cleaning.fact_extraction import CreateLayer

from .taggers.smoking_tagger import SmokingTagger
from .taggers.alcohol_tagger import AlcoholTagger


class RunBadHabits(CDAJob):
    """
    A workflow for extracting facts about bad habits: smoking and drinking.

    TODO: Add more details
    """

    prefix = luigi.Parameter()
    config_file = luigi.Parameter()

    def requires(self):

        # Check if 'texts' collection exists, if not then create it
        conn = self.create_postgres_connection(self.config_file)
        #event_collection = str(self.prefix) + "_events"
        event_collection =  "events"

        # TODO: Check that events collection exists and stop otherwise
        # Cannot be done now as the corresponding function is not implemented
        # if not self.estnltk_collection_exists(conn, event_collection, schema=self.work_schema):
        #     raise Exception("Event collection does not exist")

        task_01 = EmptyTask()

        task_02 = CreateLayer(
            prefix=self.prefix,
            config_file=self.config_file,
            work_schema=self.work_schema,
            role=self.role,
            collection=event_collection,
            layer="smoking",
            tagger=SmokingTagger(output_layer="smoking"),
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_01,
        )

        task_03 = CreateLayer(
            prefix=self.prefix,
            config_file=self.config_file,
            work_schema=self.work_schema,
            role=self.role,
            collection=event_collection,
            layer="alcohol",
            tagger=AlcoholTagger(output_layer="alcohol"),
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_02,
        )

        return [task_01, task_02, task_03]

    def run(self):
        self.log_current_action("Extract smoking and alcohol consumption")
        self.log_schemas()
        self.mark_as_complete()
