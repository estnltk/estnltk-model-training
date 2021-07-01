import luigi
import os

from cda_data_cleaning.common import get_postgres_storage
from . import luigi_targets_folder
from . import TagMeasurementTokensDev
from .taggers import MeasurementTagger


class TagMeasurementsDev(luigi.Task):
    """Create `measurements_dev` layer in the `<prefix>_events` collection.

    """

    prefix = luigi.Parameter()
    config_file = luigi.Parameter()

    def requires(self):
        return [TagMeasurementTokensDev(self.prefix, self.config_file)]

    def run(self):
        with get_postgres_storage(self.config_file) as storage:
            collection = storage[str(self.prefix) + "_events"]

            tagger = MeasurementTagger(output_layer="measurements_dev", layer_of_tokens="measurement_tokens_dev")

            collection.create_layer(tagger=tagger)

        os.makedirs(luigi_targets_folder(self.prefix), exist_ok=True)
        with self.output().open("w"):
            pass

    def output(self):
        folder = luigi_targets_folder(self.prefix)
        return luigi.LocalTarget(os.path.join(folder, "tag_measurements_dev_done"))
