import luigi
import os

from estnltk.taggers import FlattenTagger
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import get_postgres_storage


def luigi_targets_folder(prefix):
    return os.path.join("luigi_targets", prefix, "measurement_extraction", "development")


class TagMeasurementsFlat(luigi.Task):
    """Create `measurements_flat` layer in the `<prefix>_events` collection.

    """

    prefix = luigi.Parameter()
    config_file = luigi.Parameter()

    def requires(self):
        return []

    def run(self):
        with get_postgres_storage(self.config_file) as storage:
            collection = storage[str(self.prefix) + "_events"]

            tagger = FlattenTagger(
                input_layer="measurements",
                output_layer="measurements_flat",
                output_attributes=["name", "OBJECT", "VALUE", "UNIT", "MIN", "MAX", "DATE", "REGEX_TYPE"],
            )

            collection.create_layer(tagger=tagger)

        os.makedirs(luigi_targets_folder(self.prefix), exist_ok=True)
        with self.output().open("w"):
            pass

    def output(self):
        folder = luigi_targets_folder(self.prefix)
        return luigi.LocalTarget(os.path.join(folder, "tag_measurements_flat_done"))
