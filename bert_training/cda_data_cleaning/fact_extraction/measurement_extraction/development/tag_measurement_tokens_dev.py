import luigi
import os

from cda_data_cleaning.common import get_postgres_storage
from .taggers import MeasurementTokenTagger


def luigi_targets_folder(prefix):
    return os.path.join("luigi_targets", prefix, "measurement_extraction", "development")


class TagMeasurementTokensDev(luigi.Task):
    """Create `measurement_tokens_dev` layer in the events collection.

    """

    prefix = luigi.Parameter()
    config_file = luigi.Parameter()

    def requires(self):
        return []

    def run(self):
        with get_postgres_storage(self.config_file) as storage:
            collection = storage[str(self.prefix) + "_events"]

            tagger = MeasurementTokenTagger(output_layer="measurement_tokens_dev")

            collection.create_layer(tagger=tagger)

        os.makedirs(luigi_targets_folder(self.prefix), exist_ok=True)
        with self.output().open("w"):
            pass

    def output(self):
        folder = luigi_targets_folder(self.prefix)
        return luigi.LocalTarget(os.path.join(folder, "tag_measurement_tokens_dev_done"))
