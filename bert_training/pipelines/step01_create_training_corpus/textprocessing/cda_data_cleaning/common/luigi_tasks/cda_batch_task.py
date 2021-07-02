import luigi

from .cda_task import CDATask


class CDABatchTask(CDATask):
    """
    Task that combines several subtask into a single task
    Does not know what is its requirement this must be specified by upper level batch task
    TODO: Extend documentation.
    TODO: Add automatic logging
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self):
        self.log_current_action("Batch task ended")
        self.mark_as_complete()
