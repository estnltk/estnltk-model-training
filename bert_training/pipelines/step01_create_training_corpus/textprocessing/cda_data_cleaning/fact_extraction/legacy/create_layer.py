import luigi
import os

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import get_postgres_storage
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import luigi_targets_folder
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction import layer_tagger


class CreateLayer(luigi.Task):
    """Create a layer in an EstNLTK PostgreSQL collection.

    """

    prefix = luigi.Parameter()
    config_file = luigi.Parameter()
    collection = luigi.Parameter()
    layer = luigi.Parameter()
    requirement = luigi.TaskParameter()

    def requires(self):
        workers = luigi.interface.core().workers
        for i in range(workers):
            yield CreateLayerBlock(
                self.prefix, self.config_file, self.collection, self.layer, (workers, i), self.requirement
            )

    def run(self):
        os.makedirs(luigi_targets_folder(self.prefix, self.config_file, self), exist_ok=True)
        with self.output().open("w"):
            pass

    def output(self):
        folder = luigi_targets_folder(self.prefix, self.config_file, self)
        return luigi.LocalTarget(
            os.path.join(folder, "{}_collection_{}_layer_done".format(self.collection, self.layer))
        )


class CreateLayerBlock(luigi.Task):
    """Create a block of a layer table.

    """

    prefix = luigi.Parameter()
    config_file = luigi.Parameter()
    collection = luigi.Parameter()
    layer = luigi.Parameter()
    block = luigi.TupleParameter()
    requirement = luigi.TaskParameter()

    def requires(self):
        return [CreateEmptyLayerTable(self.prefix, self.config_file, self.collection, self.layer, self.requirement)]

    def run(self):
        with get_postgres_storage(self.config_file) as storage:
            collection = storage[str(self.prefix) + "_" + str(self.collection)]

            tagger = layer_tagger[self.layer]

            collection.create_layer_block(tagger=tagger, meta=None, block=self.block)

        os.makedirs(luigi_targets_folder(self.prefix, self.config_file, self), exist_ok=True)
        with self.output().open("w"):
            pass

    def output(self):
        folder = luigi_targets_folder(self.prefix, self.config_file, self)
        return luigi.LocalTarget(
            os.path.join(
                folder, "{}_collection_{}_layer_block_{}_done".format(self.collection, self.layer, self.block)
            )
        )


class CreateEmptyLayerTable(luigi.Task):
    """Create an empty table for a layer in the database.

    """

    prefix = luigi.Parameter()
    config_file = luigi.Parameter()
    collection = luigi.Parameter()
    layer = luigi.Parameter()
    requirement = luigi.TaskParameter()

    def requires(self):
        yield self.requirement

        # if self.collection == 'texts':
        #     from .event_extraction.create_texts_collection import CreateTextsCollection
        #     yield CreateTextsCollection(self.prefix, self.config_file)
        # elif self.collection == 'events':
        #     from .event_extraction.create_events_collection import CreateEventsCollection
        #     yield CreateEventsCollection(self.prefix, self.config_file)
        # elif self.collection == 'procedure_texts':
        #     from . import CreateProcedureTextsCollection
        #     yield CreateProcedureTextsCollection(self.prefix, self.config_file)
        # elif self.collection == 'procedure_subentries':
        #     from . import CreateProcedureSubentriesCollection
        #     yield CreateProcedureSubentriesCollection(self.prefix, self.config_file)

        for layer in layer_tagger[self.layer].input_layers:
            yield CreateLayer(self.prefix, self.config_file, self.collection, layer, self.requirement)

    def run(self):
        with get_postgres_storage(self.config_file) as storage:
            storage[str(self.prefix) + "_" + str(self.collection)].create_layer_table(layer_name=self.layer, meta=None)

        os.makedirs(luigi_targets_folder(self.prefix, self.config_file, self), exist_ok=True)
        with self.output().open("w"):
            pass

    def output(self):
        folder = luigi_targets_folder(self.prefix, self.config_file, self)
        return luigi.LocalTarget(
            os.path.join(folder, "{}_collection_{}_layer_empty_table_done".format(self.collection, self.layer))
        )
