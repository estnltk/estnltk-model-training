import os

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import PACKAGE_PATH
from . import read_config


def luigi_targets_folder(prefix, config_file, task):
    """
    TODO: Remove usages and delete the function.
    """
    folder = read_config(config_file)["luigi"]["folder"]

    return os.path.join(PACKAGE_PATH, "..", folder, prefix, *task.__module__.split(".")[1:])


