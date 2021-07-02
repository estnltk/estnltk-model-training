import luigi
import os

from estnltk.taggers import DiffTagger
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import get_postgres_storage

from . import TagMeasurementsDev
from . import luigi_targets_folder


class TagMeasurementsDiff(luigi.Task):
    """Create `measurements_diff` layer in the `<prefix>_events` collection.

    """

    prefix = luigi.Parameter()
    config_file = luigi.Parameter()

    def requires(self):
        return [TagMeasurementsDev(self.prefix, self.config_file)]

    def run(self):
        with get_postgres_storage(self.config_file) as storage:
            collection = storage[str(self.prefix) + "_events"]

            tagger = DiffTagger(
                layer_a="measurements",
                layer_b="measurements_dev",
                output_attributes=(),
                output_layer="measurements_diff",
            )

            collection.create_layer(
                tagger=tagger,
                overwrite=True,
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
        return luigi.LocalTarget(os.path.join(folder, "tag_measurements_diff_done"))
