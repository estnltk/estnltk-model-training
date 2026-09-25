import os
from pathlib import Path
import typing

from estnltk.converters.label_studio.labelling_configurations import (
    PhraseClassificationConfiguration,
)
from estnltk.converters.label_studio.labelling_tasks import PhraseClassificationTask


class Extractor:
    """
    Collects exported Label Studio annotation files for preprocessing.

    `extract` gathers the JSON exports from a directory and pairs each
    with its inflection type, which is read from the file name rather
    than the file contents.
    """

    def __init__(self):
        pass

    @staticmethod
    def extract(input_dir: typing.Union[str, Path]):
        """
        Find the Label Studio JSON exports under a directory.

        Files may sit directly in `input_dir` or one level down in
        subdirectories, which is how the exports are organised when each
        annotation round is kept separately.

        The inflection type is taken from the third underscore-separated
        part of the file name, as written by the export step, so names of
        the form `infl_type_16_...` are expected. A file named otherwise
        raises a ValueError rather than being skipped.

        Args:
            input_dir: directory holding the exported JSON files.

        Returns:
            A list of (inflection_type, path) pairs, in the order the
            files were found.

        Raises:
            RuntimeError: if the directory contains no JSON files.
        """
        # Collect input files
        input_files = []
        input_dir = Path(input_dir)
        for fname in os.listdir(input_dir):
            if os.path.isdir(os.path.join(input_dir, fname)):
                for subfname in os.listdir(os.path.join(input_dir, fname)):
                    if subfname.endswith(".json"):
                        inflection_type = int(
                            subfname.split("_")[2]
                        )  # infl_type_xx_1000_vx...
                        input_files.append(
                            (
                                inflection_type,
                                input_dir / fname / subfname,
                            )
                        )
            else:
                if fname.endswith(".json"):
                    inflection_type = int(
                        fname.split("_")[2]
                    )  # infl_type_xx_randomly_picked_1000_sentences...
                    input_files.append((inflection_type, input_dir / fname))

        if not input_files:
            raise RuntimeError("No input files found!")

        print(f"Found {len(input_files)} input files.")

        return input_files
