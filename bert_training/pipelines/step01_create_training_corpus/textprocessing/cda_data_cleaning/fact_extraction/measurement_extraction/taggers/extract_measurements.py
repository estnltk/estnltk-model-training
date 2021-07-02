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
    description="Run MeasurementTokenTagger and MeasurementTagger on EstNltk PostgreSQL collection.",
)


from .measurement_token_tagger import MeasurementTokenTagger
from .measurement_tagger import MeasurementTagger
from estnltk_workflows.postgres_collections import tag_collection


output_layer = args.output_layer or "measurements"
args.schema = args.schema or "grammarextractor"
args.dbname = args.dbname or "egcut_epi"
args.role = args.role or "egcut_epi_grammarextractor_create"
args.mode = args.mode or "overwrite"
args.progressbar = args.progressbar or "unicode"

measurement_token_tagger = MeasurementTokenTagger(output_layer="measurement_tokens")

tag_collection(measurement_token_tagger, args)


measurement_tagger = MeasurementTagger(layer_of_tokens="measurement_tokens", output_layer=output_layer)

tag_collection(measurement_tagger, args)
