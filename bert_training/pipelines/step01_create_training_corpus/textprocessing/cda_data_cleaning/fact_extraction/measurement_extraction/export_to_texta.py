import luigi
import os

from estnltk.converters import TextaExporter

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import get_postgres_storage, read_config
from .measurements_extraction import ExtractMeasurements
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import luigi_targets_folder

class ExportMeasurementsToTEXTA(luigi.Task):

    prefix: str = luigi.Parameter()
    config_file = luigi.Parameter()

    def requires(self):
        return [ExtractMeasurements(self.prefix, self.config_file)]

    def run(self):

        with get_postgres_storage(self.config_file) as storage:
            collection = storage[self.prefix + "_events"]
            texta_conf = read_config(self.config_file)["texta"]

            exporter = TextaExporter(
                index=self.prefix + "_events",
                doc_type="measurements",
                fact_mapping=os.path.join(os.path.dirname(__file__), "fact_mapping.csv"),
                texta_url=texta_conf["texta_url"],
                texta_username=texta_conf["texta_username"],
                texta_password=texta_conf["texta_password"],
                session_username=texta_conf["session_username"],
                session_password=texta_conf["session_password"],
            )

            events_meta = [
                "epi_id",
                "epi_type",
                "schema",
                "table",
                "field",
                "row_id",
                "effective_time",
                "header",
                "header_offset",
                "event_offset",
            ]

            with exporter.buffered_export() as buffered_export:
                for collection_id, text, meta in collection.select(
                    layers=exporter.fact_layers, progressbar=None, collection_meta=events_meta
                ):
                    meta["collection_id"] = collection_id
                    response = buffered_export(text, meta=meta)

        os.makedirs(luigi_targets_folder(self.prefix, self.config_file, self), exist_ok=True)
        with self.output().open("w"):
            pass

    def output(self):
        folder = luigi_targets_folder(self.prefix, self.config_file, self)
        return luigi.LocalTarget(os.path.join(folder, "export_measurements_to_texta_done"))
