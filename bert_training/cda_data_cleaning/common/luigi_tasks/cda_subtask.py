import luigi

from .cda_task import CDATask


class CDASubtask(CDATask):
    """
    Task that cannot be run separately. A part of a sequence of subtasks
    Does not know what is its requirement this must be specified by upper level batch task
    TODO: Extend documentation.
    """

    requirement = luigi.TaskParameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not hasattr(self, "requirement") or not isinstance(self.requirement, luigi.Task):
            raise AttributeError("Attribute requirement must be defined as luigi.TaskParameter")

    def requires(self):
        return [self.requirement]
