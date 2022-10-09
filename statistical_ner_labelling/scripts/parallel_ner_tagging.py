import configparser
import multiprocessing

from estnltk.storage import PostgresStorage
from estnltk.taggers import NerTagger

import sys
import os


class ParallelNerTagger:

    def __init__(self, dbname, user, password, host, port, role, threads, collection, morph_layer, words_layer,
                 sentences_layer, output_layer, schema):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.role = role
        self.threads = int(threads)
        self.collection = collection
        self.morph_layer = morph_layer
        self.words_layer = words_layer
        self.sentences_layer = sentences_layer
        self.output_layer = output_layer
        self.schema = schema

    def ner_thread(self, block):
        storage = PostgresStorage(dbname=self.dbname, user=self.user, password=self.password, host=self.host,
                                  port=self.port, role=self.role, schema=self.schema)
        collection = storage.get_collection(self.collection)
        morph_layer = self.morph_layer
        words_layer = self.words_layer
        sentences_layer = self.sentences_layer
        collection.selected_layers = [morph_layer, words_layer, sentences_layer]
        ner_tagger = NerTagger(output_layer=self.output_layer, morph_layer_input=morph_layer,
                               words_layer_input=words_layer,
                               sentences_layer_input=sentences_layer)

        collection.create_layer_block(tagger=ner_tagger, block=(self.threads, block))
        storage.close()

    def driver_func(self):
        processes = self.threads
        procs = []
        morph_layer = self.morph_layer
        words_layer = self.words_layer
        sentences_layer = self.sentences_layer
        output_layer = self.output_layer
        storage = PostgresStorage(dbname=self.dbname, user=self.user, password=self.password, host=self.host,
                                  port=self.port, role=self.role, schema=self.schema)
        collection = storage.get_collection(self.collection)
        ner_tagger = NerTagger(output_layer=output_layer, morph_layer_input=morph_layer,
                               words_layer_input=words_layer,
                               sentences_layer_input=sentences_layer)
        collection.add_layer(layer_template=ner_tagger.get_layer_template())
        storage.close()

        for i in range(processes):
            proc = multiprocessing.Process(target=self.ner_thread, args=(i,))
            procs.append(proc)
            proc.start()

        for proc in procs:
            proc.join()


if __name__ == '__main__':
    config = configparser.ConfigParser()
    conf_file = sys.argv[1]
    file_name = os.path.abspath(os.path.expanduser(os.path.expandvars(str(conf_file))))

    if not os.path.exists(file_name):
        raise ValueError("File {file} does not exist".format(file=str(conf_file)))

    if len(config.read(file_name)) != 1:
        raise ValueError("File {file} is not accessible or is not in valid INI format".format(file=conf_file))

    if not config.has_section('database-configuration'):
        prelude = "Error in file {}\n".format(file_name) if len(file_name) > 0 else ""
        raise ValueError("{prelude}Missing a section [{section}]".format(prelude=prelude, section='database-configuration'))
    for option in ["server", "port", "database", "username", "password","role","schema","collection","morph_layer",
                   "words_layer","sentences_layer"]:
        if not config.has_option('database-configuration', option):
            prelude = "Error in file {}\n".format(file_name) if len(file_name) > 0 else ""
            raise ValueError(
                "{prelude}Missing option {option} in the section [{section}]".format(
                    prelude=prelude, option=option, section='database-configuration'
                )
            )
    if not config.has_section('ner-configuration'):
        prelude = "Error in file {}\n".format(file_name) if len(file_name) > 0 else ""
        raise ValueError("{prelude}Missing a section [{section}]".format(prelude=prelude, section='ner-configuration'))
    for option in ["num_threads","output_layer"]:
        if not config.has_option('ner-configuration', option):
            prelude = "Error in file {}\n".format(file_name) if len(file_name) > 0 else ""
            raise ValueError(
                "{prelude}Missing option {option} in the section [{section}]".format(
                    prelude=prelude, option=option, section='ner-configuration'
                )
            )

    config.read(file_name)

    parallel_ner_tagger = ParallelNerTagger(dbname=config['database-configuration']['database'],
                                            user=config['database-configuration']['username'],
                                            password=config['database-configuration']['password'],
                                            host=config['database-configuration']['server'],
                                            port=config['database-configuration']['port'],
                                            role=config['database-configuration']['role'],
                                            threads=config['ner-configuration']['num_threads'],
                                            collection=config['database-configuration']['collection'],
                                            morph_layer=config['database-configuration']['morph_layer'],
                                            words_layer=config['database-configuration']['words_layer'],
                                            sentences_layer=config['database-configuration']['sentences_layer'],
                                            output_layer=config['ner-configuration']['output_layer'],
                                            schema=config['database-configuration']['schema'])

    parallel_ner_tagger.driver_func()
