import luigi

from cda_data_cleaning.common.luigi_tasks import CDASubtask
from cda_data_cleaning.common.luigi_parameters import ObjectParameter


class CreateLayer(CDASubtask):
    """Create a layer in an EstNLTK PostgreSQL collection.

    1. Creates layer table as EstNLTK storage
    2. Creates layers to given collection using given tagger
    """

    prefix = luigi.Parameter()
    config_file = luigi.Parameter()
    work_schema = luigi.Parameter()
    role = luigi.Parameter()
    collection = luigi.Parameter()
    layer = luigi.Parameter()
    tagger = ObjectParameter()
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    def requires(self):
        print("\n\nCreate layer requires ", self.layer, "\n\n")
        table_name = str(self.prefix) + "_" + str(self.collection) + "__" + str(self.layer) + "__layer"
        print("TAble name", table_name)

        conn = self.create_postgres_connection(self.config_file)
        db_table_exists = self.db_table_exists(conn=conn, table=table_name, schema=self.work_schema)
        conn.close()

        print("Does table exist", db_table_exists)
        if not db_table_exists:
            storage = self.create_estnltk_storage(
                config=self.config_file, work_schema=self.work_schema, role=self.role
            )
            storage[str(self.prefix) + "_" + str(self.collection)].create_layer_table(layer_name=self.layer, meta=None)

        workers = luigi.interface.core().workers
        for i in range(workers):
            yield CreateLayerBlock(
                prefix=self.prefix,
                config_file=self.config_file,
                work_schema=self.work_schema,
                role=self.role,
                collection=self.collection,
                layer=self.layer,
                tagger=self.tagger,
                block=(workers, i),
                luigi_targets_folder=self.luigi_targets_folder,
                requirement=self.requirement,
            )

    def run(self):
        self.mark_as_complete()


class CreateLayerBlock(CDASubtask):
    """Create a block of a layer table.

    """

    prefix = luigi.Parameter()
    config_file = luigi.Parameter()
    collection = luigi.Parameter()
    work_schema = luigi.Parameter()
    role = luigi.Parameter()
    layer = luigi.Parameter()
    tagger = ObjectParameter()
    block = luigi.TupleParameter()
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    def run(self):
        self.log_current_action("CreateLayerBlock_" + self.layer + "; tagger" + str(self.tagger))
        self.log_schemas()

        storage = self.create_estnltk_storage(config=self.config_file, work_schema=self.work_schema, role=self.role)

        collection = storage[str(self.prefix) + "_" + str(self.collection)]
        tagger = self.tagger

        collection.create_layer_block(tagger=tagger, meta=None, block=self.block)

        self.mark_as_complete()
