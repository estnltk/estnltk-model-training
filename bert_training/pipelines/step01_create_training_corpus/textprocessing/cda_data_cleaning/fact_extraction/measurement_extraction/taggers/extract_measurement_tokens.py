from estnltk_workflows.postgres_collections import parse_args


args = parse_args(
    "collection",
    "pgpass",
    "host",
    "port",
    "dbname",
    "user",
    "schema",
    "role",
    "mode",
    "progressbar",
    "logging",
    "output_layer",
    description="Run MeasurementTagger on EstNltk PostgreSQL collection.",
)


from .measurement_token_tagger import MeasurementTokenTagger
from estnltk_workflows.postgres_collections import tag_collection


tagger = MeasurementTokenTagger(output_layer=args.output_layer)

tag_collection(tagger, args)
