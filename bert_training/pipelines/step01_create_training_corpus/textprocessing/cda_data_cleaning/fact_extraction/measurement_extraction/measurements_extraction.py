import luigi

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction import CreateLayer
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import CDASubtask
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction.measurement_extraction.taggers import MeasurementTagger
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction.measurement_extraction.taggers import MeasurementTokenTagger


class ExtractMeasurements(CDASubtask):
    """Creates `measurement_tokens` and `measurement` layers in the `events` collection.

    """

    prefix = luigi.Parameter()
    config_file = luigi.Parameter()
    schema = luigi.Parameter()
    role = luigi.Parameter()

    def requires(self):
        # events = CreateLayer(
        #    self.prefix, self.config_file, "events", "measurements", ExtractEvents(self.prefix, self.config_file)
        # )
        #
        # type1_layer = CreateLayer(
        #     self.prefix,
        #     self.config_file,
        #     "events",
        #     "analysis_print_tables_type1",
        #     events
        #     # ExtractEvents(self.prefix, self.config_file)
        # )
        # type2_layer = CreateLayer(self.prefix, self.config_file, "events", "analysis_print_tables_type2", type1_layer)
        #
        # type3_layer = CreateLayer(self.prefix, self.config_file, "events", "analysis_print_tables_type3", type2_layer)
        #
        # type4_layer = CreateLayer(self.prefix, self.config_file, "events", "analysis_print_tables_type4", type3_layer)
        #
        # type5_layer = CreateLayer(self.prefix, self.config_file, "events", "analysis_print_tables_type5", type4_layer)
        #
        # type6_layer = CreateLayer(self.prefix, self.config_file, "events", "analysis_print_tables_type6", type5_layer)
        #
        # type7_layer = CreateLayer(self.prefix, self.config_file, "events", "analysis_print_tables_type7", type6_layer)
        #

        task_00 = self.requirement

        # TODO: Currently, RunExtractMeasurementsToTable expects ExtractEvents to be completed.
        #       Another workflow needs to be created that extracts the events before extracting the measurements
        #       and exporting them to a table.

        # Check if 'texts' collection exists, if not then create it
        conn = self.create_postgres_connection(self.config_file)
        # event_collection = str(self.prefix) + "_events"
        event_collection = "events"

        # @TODO - checks in requires field do not allow for master workflow to operate

        # if not self.estnltk_collection_exists(conn, event_collection, schema=str(self.schema)):
        #     ExtractEvents(prefix=self.prefix, config_file=self.config_file)
            
        conn.close()

        task_01 = CreateLayer(
            prefix=self.prefix,
            config_file=self.config_file,
            work_schema=self.schema,
            role=self.role,
            collection=event_collection,
            layer="measurement_tokens",
            tagger=MeasurementTokenTagger(output_layer="measurement_tokens"),
            requirement=task_00
        )

        task_02 = CreateLayer(
            prefix=self.prefix,
            config_file=self.config_file,
            work_schema=self.schema,
            role=self.role,
            collection=event_collection,
            layer="measurements",
            tagger=MeasurementTagger(output_layer="measurements"),
            requirement=task_01
        )

        return [
            task_00, task_01, task_02
            # printed analysis tables locations in table's text field
            # CreateLayer(self.prefix, self.config_file, "events", "analysis_print_tables", type7_layer),
        ]

    def run(self):
        self.mark_as_complete()
