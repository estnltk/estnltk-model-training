import configparser
import multiprocessing

from estnltk.storage import PostgresStorage
from estnltk.taggers import NerTagger

class ParallelNerTagger:

    def __init__(self, dbname, user, password, host, port, role, threads, collection, morph_layer, words_layer,
                 sentences_layer):
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

    def ner_thread(self, block):
        storage = PostgresStorage(dbname=self.dbname, user=self.user, password=self.password, host=self.host,
                                  port=self.port, role=self.role)
        collection = storage.get_collection(self.collection)
        morph_layer = self.morph_layer
        words_layer = self.words_layer
        sentences_layer = self.sentences_layer
        collection.selected_layers = [morph_layer, words_layer, sentences_layer]
        ner_tagger = NerTagger(morph_layer_input=morph_layer, words_layer_input=words_layer,
                               sentences_layer_input=sentences_layer)
        # change the number based on the number of parallel threads and also in the driver_func
        collection.create_layer_block(tagger=ner_tagger, block=(self.threads, block))

    def driver_func(self):
        # change the number based on the number of parallel threads and also in the ner_thread
        processes = self.threads
        procs = []
        for i in range(processes):
            proc = multiprocessing.Process(target=self.ner_thread, args=(i,))
            procs.append(proc)
            proc.start()

        for proc in procs:
            proc.join()


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('workflow_config.ini')

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
                                            sentences_layer=config['database-configuration']['sentences_layer'])

    parallel_ner_tagger.driver_func()
