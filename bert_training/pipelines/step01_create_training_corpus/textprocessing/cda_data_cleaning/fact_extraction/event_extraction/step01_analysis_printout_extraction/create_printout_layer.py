import luigi

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import CDASubtask
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction import CreateLayer
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction import (
    Type1StartEndTagger,
    Type2StartEndTagger,
    Type3StartEndTagger,
    Type4StartEndTagger,
    Type5StartEndTagger,
    Type6StartEndTagger,
    Type7StartEndTagger,
    SegmentsTagger,
)


class CreatePrintoutLayer(CDASubtask):
    """Tag printout layer in the `<prefix>_texts` collection.
    """

    prefix = luigi.Parameter()
    config_file = luigi.Parameter()
    schema = luigi.Parameter()
    role = luigi.Parameter()
    collection = luigi.Parameter()
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    def requires(self):

        # check if 'texts' collection exists, if not then create it
        conn = self.create_postgres_connection(self.config_file)
        db_table_exists = self.db_table_exists(conn=conn, table=str(self.prefix) + "_texts", schema=self.schema)

        task_01 = self.requirement

        # if not db_table_exists:
        #     print("\n\n\nDb table does not exist!")
        #     # should be resolved:
        #     # problem here is task_02 starts running before task_01 is complete for some mysterious reason
        #     # task_01 = CreateTextsCollection2(prefix=self.prefix, config_file=self.config_file)
        #     error = "Tables {schema}.{prefix}_texts and {schema}.{prefix}_texts__structure do not exist, first run workflow in fact_extraction/text_extraction and then try again!".format(
        #         schema=self.schema, prefix=self.prefix
        #     )
        #     raise Exception(error)

        task_02 = CreateLayer(
            prefix=self.prefix,
            config_file=self.config_file,
            work_schema=self.schema,
            role=self.role,
            collection=self.collection,
            layer="printout_type1",
            tagger=Type1StartEndTagger(output_layer="printout_type1"),
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_01,
        )

        task_03 = CreateLayer(
            prefix=self.prefix,
            config_file=self.config_file,
            work_schema=self.schema,
            role=self.role,
            collection=self.collection,
            layer="printout_type2",
            tagger=Type2StartEndTagger(output_layer="printout_type2"),
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_02,
        )

        task_04 = CreateLayer(
            prefix=self.prefix,
            config_file=self.config_file,
            work_schema=self.schema,
            role=self.role,
            collection=self.collection,
            layer="printout_type3",
            tagger=Type3StartEndTagger(output_layer="printout_type3"),
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_03,
        )

        task_05 = CreateLayer(
            prefix=self.prefix,
            config_file=self.config_file,
            work_schema=self.schema,
            role=self.role,
            collection=self.collection,
            layer="printout_type4",
            tagger=Type4StartEndTagger(output_layer="printout_type4"),
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_04,
        )

        task_06 = CreateLayer(
            prefix=self.prefix,
            config_file=self.config_file,
            work_schema=self.schema,
            role=self.role,
            collection=self.collection,
            layer="printout_type5",
            tagger=Type5StartEndTagger(output_layer="printout_type5"),
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_05,
        )

        task_07 = CreateLayer(
            prefix=self.prefix,
            config_file=self.config_file,
            work_schema=self.schema,
            role=self.role,
            collection=self.collection,
            layer="printout_type6",
            tagger=Type6StartEndTagger(output_layer="printout_type6"),
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_06,
        )

        task_08 = CreateLayer(
            prefix=self.prefix,
            config_file=self.config_file,
            work_schema=self.schema,
            role=self.role,
            collection=self.collection,
            layer="printout_type7",
            tagger=Type7StartEndTagger(output_layer="printout_type7"),
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_07,
        )

        task_09 = CreateLayer(
            prefix=self.prefix,
            config_file=self.config_file,
            work_schema=self.schema,
            role=self.role,
            collection=self.collection,
            layer="printout_segments",
            tagger=SegmentsTagger(output_layer="printout_segments"),
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_08,
        )

        return [task_01, task_02, task_03, task_04, task_05, task_06, task_07, task_08, task_09]

    def run(self):
        self.mark_as_complete()
