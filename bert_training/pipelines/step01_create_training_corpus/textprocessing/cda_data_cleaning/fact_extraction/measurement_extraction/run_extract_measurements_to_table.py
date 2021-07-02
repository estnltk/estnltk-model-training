import luigi

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import CDAJob

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction.measurement_extraction.measurements_extraction import ExtractMeasurements
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction.measurement_extraction.export_to_table import ExportMeasurementsToTable


class RunExtractMeasurementsToTable(CDAJob):
    prefix = luigi.Parameter(default="")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def requires(self):
        self.log_current_time("Starting time")

        task_00 = self.requirement

        # @TODO - checks in requires field do not allow for master workflow to operate

        # if not self.estnltk_collection_exists(
        #         self.create_postgres_connection(self.config_file),
        #         str(self.prefix) + "_events",
        #         schema=str(self.work_schema)
        # ):
        #     raise Exception("Collection 'Events' does not exist")

        task_01 = ExtractMeasurements(
            config_file=self.config_file,
            prefix=self.prefix,
            schema=self.work_schema,
            role=self.role,
            requirement=task_00
        )

        task_02 = ExportMeasurementsToTable(
            config_file=self.config_file,
            prefix=self.prefix,
            schema=self.work_schema,
            role=self.role,
            requirement=task_01
        )

        return [
            task_00,
            task_01,
            task_02
        ]

    def run(self):
        self.log_current_time("Ending time")
        self.mark_as_complete()
