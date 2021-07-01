import os


def luigi_targets_folder(prefix):
    return os.path.join("luigi_targets", prefix, "fact_extraction", "measurement_extraction", "development")
