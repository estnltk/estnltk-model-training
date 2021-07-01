import luigi
import os

from estnltk.taggers import DiffTagger
from cda_data_cleaning.common import get_postgres_storage

from . import TagMeasurementsFlat
from . import TagMeasurementsDev


def luigi_targets_folder(prefix):
    return os.path.join("luigi_targets", prefix, "measurement_extraction", "development")


class TagMeasurementsFlatDiff(luigi.Task):
    """Create `measurements_flat_diff` layer in the `<prefix>_events` collection.

    """

    prefix = luigi.Parameter()
    config_file = luigi.Parameter()

    def requires(self):
        return [TagMeasurementsFlat(self.prefix, self.config_file), TagMeasurementsDev(self.prefix, self.config_file)]

    def run(self):
        with get_postgres_storage(self.config_file) as storage:
            collection = storage[str(self.prefix) + "_events"]

            tagger = DiffTagger(
                layer_a="measurements_flat",
                layer_b="measurements_dev",
                output_attributes=["name", "OBJECT", "VALUE", "UNIT", "MIN", "MAX", "DATE", "REGEX_TYPE"],
                output_layer="measurements_flat_diff",
            )

            collection.create_layer(
                tagger=tagger,
                meta={
                    "modified_spans": "int",
                    "missing_spans": "int",
                    "extra_spans": "int",
                    "extra_annotations": "int",
                    "missing_annotations": "int",
                    "overlapped": "int",
                    "prolonged": "int",
                    "shortened": "int",
                },
            )

        os.makedirs(luigi_targets_folder(self.prefix), exist_ok=True)
        with self.output().open("w"):
            pass

    def output(self):
        folder = luigi_targets_folder(self.prefix)
        return luigi.LocalTarget(os.path.join(folder, "tag_measurements_flat_diff_done"))
