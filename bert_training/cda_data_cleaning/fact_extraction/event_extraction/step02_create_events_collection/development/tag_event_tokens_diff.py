import luigi

import os

from cda_data_cleaning.common.luigi_targets import luigi_targets_folder
from cda_data_cleaning.fact_extraction.legacy import CreateLayer


class TagEventTokensDiff(luigi.Task):
    """Create `event_tokens_diff` layer in the `<prefix>_texts` collection.

    """

    prefix = luigi.Parameter()
    config_file = luigi.Parameter()

    def requires(self):
        return [CreateLayer(self.prefix, self.config_file, "texts", "event_tokens_diff")]

    def run(self):
        os.makedirs(luigi_targets_folder(self.prefix), exist_ok=True)
        with self.output().open("w"):
            pass

    def output(self):
        folder = luigi_targets_folder(self.prefix)
        return luigi.LocalTarget(os.path.join(folder, "tag_measurements_diff_done"))
