import luigi

from cda_data_cleaning.common.luigi_tasks import CDATask


class EmptyTask(CDATask):
    """
    Sometimes luigi tasks require the "requirement" parameter to be set. As it needs to be type TaskParameter()
    then empty task can be give (None, [], "" give and error message). This task does absolutely nothing.
    """

    # prefix = luigi.Parameter()
    # config_file = luigi.Parameter()
    # luigi_targets_folder = luigi.Parameter(default=".")

    def run(self):
        self.mark_as_complete()
