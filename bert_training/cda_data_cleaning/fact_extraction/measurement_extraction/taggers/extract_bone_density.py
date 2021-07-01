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
    "input_layer",
    "output_layer",
    description="Run MeasurementTagger on EstNltk PostgreSQL collection.",
)


from estnltk_workflows.postgres_collections import tag_collection
from .bone_density_tagger import BoneDensityTagger


tagger = BoneDensityTagger(output_layer=args.output_layer)

tag_collection(tagger, args)
