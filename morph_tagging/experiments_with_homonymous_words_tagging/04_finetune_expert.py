#
#   Fine-tunes the form homonymy expert model.
#
#   Starts from a BERT-based morphological tagging model (Est-RoBERTa
#   fine-tuned on Vabamorf tags, published as 'bert_morph_v2') and
#   continues training it on the word form homonymy corpus, so that it
#   specialises in disambiguating homonymous forms of inflection types
#   1, 16, 17 and 19.
#
#   The result is an expert model, not a general tagger: specialising
#   this way costs it a great deal of general morphological accuracy
#   (catastrophic forgetting), so it is meant to be used as the expert
#   component of a mixture of experts. See readme.md for the figures.
#
#   Example:
#
#     python 04_finetune_expert.py \
#         --train-set   data/homonyms_train.parquet \
#         --eval-set    data/homonyms_dev.parquet \
#         --labels      ../unique_labels.json \
#         --base-model  bert_morph_v2 \
#         --output-dir  models/homonym_expert
#
#   --base-model accepts either a local checkpoint directory or the
#   name of an EstNLTK resource, which is downloaded if missing.
#

import argparse
import json
import sys
from pathlib import Path

from model_utils import initialize_model, load_df
from training import train_token_classification


def parse_args() -> argparse.Namespace:
    """Parse the command line arguments.

    Returns
    -------
    argparse.Namespace
        The parsed arguments.
    """
    parser = argparse.ArgumentParser(
        description="Fine-tune the form homonymy expert model."
    )
    parser.add_argument("--train-set", type=Path, required=True,
                        help="Token-level training data (parquet/csv/json) with "
                             "columns sentence_id, words, labels.")
    parser.add_argument("--eval-set", type=Path, default=None,
                        help="Optional validation data, same format. Required for "
                             "early stopping.")
    parser.add_argument("--labels", type=Path, default=Path("../unique_labels.json"),
                        help="JSON list of all label strings. Defaults to the shared "
                             "label set one directory up.")
    parser.add_argument("--base-model", type=str, default="bert_morph_v2",
                        help="Checkpoint directory to start from, or an EstNLTK "
                             "resource name.")
    parser.add_argument("--output-dir", type=Path, required=True,
                        help="Where the fine-tuned model is written.")
    parser.add_argument("--best-model-dir", type=Path, default=None,
                        help="Where the best checkpoint is written, if evaluating.")
    parser.add_argument("--epochs", type=int, default=10)
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--learning-rate", type=float, default=5e-5)
    parser.add_argument("--max-length", type=int, default=None)
    parser.add_argument("--patience", type=int, default=2,
                        help="Epochs without improvement before early stopping.")
    parser.add_argument("--no-early-stopping", action="store_true")
    # The homonymy corpus labels only the homonymous word in each sentence and
    # marks every other token '-', so placeholders are the norm here rather than
    # the exception. Training would otherwise refuse the data, because '-' is not
    # in the model's label mapping.
    parser.add_argument("--keep-placeholders", action="store_true",
                        help="Treat placeholder labels ('-', 'NONE', '') as real "
                             "labels. Off by default: the homonymy corpus labels "
                             "only the target word and marks the rest '-'.")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--dry-run", action="store_true",
                        help="Run the training loop without writing any model files.")
    return parser.parse_args()


def resolve_base_model(name: str) -> str:
    """Return a local path for a checkpoint directory or an EstNLTK resource name."""

    if Path(name).exists():
        return str(name)
    # Not a path, so treat it as a resource alias and fetch it if needed.
    from estnltk.downloader import get_resource_paths

    resolved = get_resource_paths(name, only_latest=True, download_missing=True)
    if resolved is None:
        raise SystemExit(
            f"(!) '{name}' is neither an existing directory nor a known EstNLTK "
            f"resource. Pass a checkpoint directory, or one of the published model "
            f"names such as 'bert_morph_v2'."
        )
    return str(resolved)


def main() -> None:
    """Fine-tune the expert model and report the training statistics."""
    args = parse_args()

    if args.eval_set is None and not args.no_early_stopping:
        raise SystemExit(
            "(!) Early stopping needs a validation set. Pass --eval-set, or disable "
            "it with --no-early-stopping."
        )

    label_list = json.loads(args.labels.read_text(encoding="utf-8"))
    if not isinstance(label_list, list):
        raise SystemExit(f"(!) {args.labels} should contain a JSON list of labels.")

    base_model = resolve_base_model(args.base_model)
    print(f"Base model : {base_model}")
    print(f"Labels     : {len(label_list)} from {args.labels}")

    train_df = load_df(args.train_set, file_format="auto")
    eval_df = load_df(args.eval_set, file_format="auto") if args.eval_set else None
    print(f"Train rows : {len(train_df)}")
    if eval_df is not None:
        print(f"Eval rows  : {len(eval_df)}")

    bundle = initialize_model(base_model, unique_labels=label_list, cleanup=True)

    stats = train_token_classification(
        model=bundle["model"],
        tokenizer=bundle["tokenizer"],
        train_df=train_df,
        label_list=label_list,
        output_dir=str(args.output_dir),
        eval_df=eval_df,
        num_train_epochs=args.epochs,
        train_batch_size=args.batch_size,
        learning_rate=args.learning_rate,
        max_length=args.max_length,
        evaluate_during_training=eval_df is not None,
        use_early_stopping=not args.no_early_stopping,
        patience_n=args.patience,
        best_model_dir=str(args.best_model_dir) if args.best_model_dir else None,
        ignore_placeholders=not args.keep_placeholders,
        seed=args.seed,
        dry_run=args.dry_run,
    )

    print(json.dumps(stats, indent=2, default=str))


if __name__ == "__main__":
    main()