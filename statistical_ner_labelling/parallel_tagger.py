import configparser
import copy
import multiprocessing

from estnltk.storage import PostgresStorage
from estnltk.taggers import *

import os


class ParallelTagger:

    def __init__(self):
        super(ParallelTagger, self).__init__()

    def ner_thread(self, block, tagger,**kwargs):
        storage = PostgresStorage(dbname=self.dbname,
                                            user=self.user,
                                            password=self.password,
                                            host=self.host,
                                            port=self.port,
                                            role=self.role,
                                            schema=self.schema)
        collection = storage.get_collection(self.collection)
        classname = globals()[tagger]
        tagger = classname(output_layer='ner2')

        collection.create_layer_block(tagger=tagger, block=(self.num_threads, block))
        storage.close()

    def driver_func(self, tagger,**kwargs):
        processes = self.num_threads
        procs = []
        storage = PostgresStorage(dbname=self.dbname,
                                            user=self.user,
                                            password=self.password,
                                            host=self.host,
                                            port=self.port,
                                            role=self.role,
                                            schema=self.schema)
        collection = storage.get_collection(self.collection)
        classname = globals()[tagger]
        tagger_obj = classname(**kwargs)
        collection.add_layer(layer_template=tagger_obj.get_layer_template())
        storage.close()

        for i in range(processes):
            proc = multiprocessing.Process(target=self.ner_thread, args=(i, tagger,kwargs))
            procs.append(proc)
            proc.start()

        for proc in procs:
            proc.join()

    def parallel_tagger(self, tagger, config_file: str,**kwargs):
        config = configparser.ConfigParser()
        file_name = os.path.abspath(os.path.expanduser(os.path.expandvars(str(config_file))))

        if not os.path.exists(file_name):
            raise ValueError("File {file} does not exist".format(file=str(config_file)))

        if len(config.read(file_name)) != 1:
            raise ValueError("File {file} is not accessible or is not in valid INI format".format(file=config_file))

        if not config.has_section('database-configuration'):
            prelude = "Error in file {}\n".format(file_name) if len(file_name) > 0 else ""
            raise ValueError(
                "{prelude}Missing a section [{section}]".format(prelude=prelude, section='database-configuration'))
        for option in ["server", "port", "database", "username", "password", "role", "schema", "collection",
                       "morph_layer",
                       "words_layer", "sentences_layer"]:
            if not config.has_option('database-configuration', option):
                prelude = "Error in file {}\n".format(file_name) if len(file_name) > 0 else ""
                raise ValueError(
                    "{prelude}Missing option {option} in the section [{section}]".format(
                        prelude=prelude, option=option, section='database-configuration'
                    )
                )
        if not config.has_section('ner-configuration'):
            prelude = "Error in file {}\n".format(file_name) if len(file_name) > 0 else ""
            raise ValueError(
                "{prelude}Missing a section [{section}]".format(prelude=prelude, section='ner-configuration'))
        for option in ["num_threads", "output_layer"]:
            if not config.has_option('ner-configuration', option):
                prelude = "Error in file {}\n".format(file_name) if len(file_name) > 0 else ""
                raise ValueError(
                    "{prelude}Missing option {option} in the section [{section}]".format(
                        prelude=prelude, option=option, section='ner-configuration'
                    )
                )

        config.read(file_name)

        self.dbname = config['database-configuration']['database']
        self.user = config['database-configuration']['username']
        self.password = config['database-configuration']['password']
        self.host = config['database-configuration']['server']
        self.port = config['database-configuration']['port']
        self.role = config['database-configuration']['role']
        self.schema = config['database-configuration']['schema']
        self.num_threads = int(config['ner-configuration']['num_threads'])
        self.collection = config['database-configuration']['collection']
        self.driver_func(tagger,**kwargs)