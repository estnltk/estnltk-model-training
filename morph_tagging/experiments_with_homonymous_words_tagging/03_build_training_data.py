#
#   Turns Label Studio annotations into the token-level dataset the
#   expert model is fine-tuned on.
#
#   Runs the three steps between annotation and training:
#     1) collect the exported Label Studio JSON files, grouped by
#        inflection type;
#     2) turn them into token-level rows, one per word, carrying the
#        form chosen by the annotator for the homonymous word;
#     3) split those rows into train and test by sentence.
#
#   A sentence-level view of the same annotations is written alongside,
#   which is convenient for inspection but is not what the model trains
#   on.
#
#   Every token other than the annotated homonymous word is labelled
#   '-'. That is deliberate: the expert is trained to decide homonymous
#   forms, not to tag whole sentences, and 04_finetune_expert.py ignores
#   those placeholder labels by default.
#
#   Example:
#
#     python 03_build_training_data.py \
#         --annotations-dir from_labelstudio \
#         --output-dir      data
#
#   Writes into --output-dir:
#     homonyms_overall.parquet             token level, all inflection types
#     homonyms_overall_sentences.parquet   sentence level, for inspection
#     homonyms_train.parquet               token level, training split
#     homonyms_test.parquet                token level, test split
#

import argparse
from pathlib import Path

import pandas as pd

from extract import Extractor
from preprocess import Preprocessor

MODEL_DATASET_FILE = "homonyms_overall.parquet"
SENTENCES_FILE = "homonyms_overall_sentences.parquet"
TRAIN_FILE = "homonyms_train.parquet"
TEST_FILE = "homonyms_test.parquet"


def parse_args() -> argparse.Namespace:
    """Parse the command line arguments.

    Returns
    -------
    argparse.Namespace
        The parsed arguments.
    """
    parser = argparse.ArgumentParser(
        description="Build the expert model's training data from Label Studio "
                    "annotations."
    )
    parser.add_argument("--annotations-dir", type=Path, required=True,
                        help="Directory of exported Label Studio JSON files. Files "
                             "may sit directly in it or in one level of "
                             "subdirectories; the inflection type is read from each "
                             "file name.")
    parser.add_argument("--output-dir", type=Path, required=True,
                        help="Where the parquet files are written.")
    parser.add_argument("--test-size", type=float, default=0.2,
                        help="Fraction of sentences held out for testing.")
    parser.add_argument("--seed", type=int, default=42,
                        help="Seed for the train/test split.")
    parser.add_argument("--individual-dfs", action="store_true",
                        help="Also write a separate sentence-level file per "
                             "inflection type.")
    return parser.parse_args()


def main() -> None:
    """Build the training data and write it into the output directory."""
    args = parse_args()

    if not args.annotations_dir.is_dir():
        raise SystemExit(f"(!) Not a directory: {args.annotations_dir}")
    args.output_dir.mkdir(parents=True, exist_ok=True)

    input_files = Extractor.extract(args.annotations_dir)
    if not input_files:
        raise SystemExit(
            f"(!) No Label Studio JSON files found under {args.annotations_dir}. "
            f"File names are expected to carry the inflection type, as written by "
            f"02_export_to_labelstudio.py."
        )
    by_type = sorted({infl_type for infl_type, _ in input_files})
    print(f"Annotation files : {len(input_files)} across inflection types {by_type}")

    # Both writers save into output_dir rather than returning a frame.
    # create_model_df produces the token-level rows the model is trained on:
    # sentence_id, words, form, pos, labels, infl_type, source.
    Preprocessor.create_model_df(
        input_files=input_files,
        output_dir=args.output_dir,
        do_overall_df=True,
        do_individual_dfs=args.individual_dfs,
    )
    model_path = args.output_dir / MODEL_DATASET_FILE
    if not model_path.exists():
        raise SystemExit(f"(!) Expected {model_path} to be written, but it is not there.")
    model_df = pd.read_parquet(model_path)
    print(f"Token level rows   : {len(model_df)}")

    # create_model_df names the label column 'label', while the training and
    # evaluation scripts default to 'labels' (model_utils.prepare_shared_inputs).
    # Rename here so the files this script writes feed straight into
    # 04_finetune_expert.py, and so they match the column names of the published dataset.
    if "label" in model_df.columns and "labels" not in model_df.columns:
        model_df = model_df.rename(columns={"label": "labels"})
        model_df.to_parquet(model_path, index=False)

    # Sentence-level view of the same annotations, for inspection only.
    Preprocessor.create_sentences_df(
        input_files=input_files,
        output_dir=args.output_dir,
        do_overall_df=True,
        do_individual_dfs=args.individual_dfs,
    )

    train_df, test_df = Preprocessor.train_test_split(
        model_df, test_size=args.test_size, seed=args.seed, label_col="labels"
    )
    train_df.to_parquet(args.output_dir / TRAIN_FILE, index=False)
    test_df.to_parquet(args.output_dir / TEST_FILE, index=False)
    print(f"Train rows         : {len(train_df)} -> {args.output_dir / TRAIN_FILE}")
    print(f"Test rows          : {len(test_df)} -> {args.output_dir / TEST_FILE}")


if __name__ == "__main__":
    main()