import pandas as pd
import os

from estnltk.storage import PostgresStorage

import sys
import configparser


class EntityCounting:

    def __init__(self, input_layer_name='ner', output_file='data/ner_counts.csv', lemmas='True', port=None, role=None,
                 collection=None, user=None, password=None, host=None, dbname=None, schema=None):
        self.role = role
        self.collection = collection
        self.port = port
        self.user = user
        self.password = password
        self.host = host
        self.dbname = dbname
        self.input_layer_name = input_layer_name
        self.output_file = output_file
        self.lemmas = bool(lemmas)
        self.schema = schema

    def create_csv(self):
        entity_counts = {}

        storage = PostgresStorage(dbname=self.dbname,
                                  user=self.user, password=self.password, host=self.host,
                                  port=self.port, role=self.role, schema=self.schema)
        collection = storage.get_collection(self.collection)

        collection.selected_layers = ['ner']
        for text in collection:
            for match in text[self.input_layer_name]:
                current = entity_counts.get((tuple(match.text), match.nertag), 0)
                entity_counts[(tuple(match.text), match.nertag)] = current + 1

        df = pd.Series(entity_counts).reset_index()
        df.columns = ['entity', 'label', 'entity_count']
        df.to_csv(self.output_file)


if __name__ == '__main__':
    config = configparser.ConfigParser()
    # first command line argument is the config file
    conf_file = sys.argv[1]
    file_name = os.path.abspath(os.path.expanduser(os.path.expandvars(str(conf_file))))

    if not os.path.exists(file_name):
        raise ValueError("File {file} does not exist".format(file=str(conf_file)))

    if len(config.read(file_name)) != 1:
        raise ValueError("File {file} is not accessible or is not in valid INI format".format(file=conf_file))

    if not config.has_section('database-configuration'):
        prelude = "Error in file {}\n".format(file_name) if len(file_name) > 0 else ""
        raise ValueError("{prelude}Missing a section [{section}]".format(prelude=prelude, section='database-configuration'))
    for option in ["server", "port", "database", "username", "password","role","schema","collection"]:
        if not config.has_option('database-configuration', option):
            prelude = "Error in file {}\n".format(file_name) if len(file_name) > 0 else ""
            raise ValueError(
                "{prelude}Missing option {option} in the section [{section}]".format(
                    prelude=prelude, option=option, section='database-configuration'
                )
            )
    if not config.has_section('entity-counting'):
        prelude = "Error in file {}\n".format(file_name) if len(file_name) > 0 else ""
        raise ValueError("{prelude}Missing a section [{section}]".format(prelude=prelude, section='entity-counting'))
    for option in ["input_layer","output_file","lemmas"]:
        if not config.has_option('entity-counting', option):
            prelude = "Error in file {}\n".format(file_name) if len(file_name) > 0 else ""
            raise ValueError(
                "{prelude}Missing option {option} in the section [{section}]".format(
                    prelude=prelude, option=option, section='entity-counting'
                )
            )

    config.read(file_name)

    entity_counting = EntityCounting(input_layer_name=config['entity-counting']['input_layer'],
                                     output_file=config['entity-counting']['output_file'],
                                     lemmas=config['entity-counting']['lemmas'],
                                     dbname=config['database-configuration']['database'],
                                     user=config['database-configuration']['username'],
                                     password=config['database-configuration']['password'],
                                     host=config['database-configuration']['server'],
                                     port=config['database-configuration']['port'],
                                     role=config['database-configuration']['role'],
                                     collection=config['database-configuration']['collection'],
                                     schema=config['database-configuration']['schema']
                                     )
    entity_counting.create_csv()
