import pandas as pd
import argparse


class EntityCounting:

    def __init__(self,collection,input_layer_name='ner', output_file='ner_counts.csv',lemmas='True'):
        self.collection = collection
        self.input_layer_name = input_layer_name
        self.output_file = output_file
        self.lemmas = bool(lemmas)

    def create_csv(self):
        entity_counts = {}

        for text in self.collection:
            for match in getattr(text,self.input_layer_name):
                current = entity_counts.get((tuple(match.text),match.nertag), 0)
                entity_counts[(tuple(match.text),match.nertag)] = current + 1

        df = pd.Series(entity_counts).reset_index()
        df.columns = ['entity', 'label', 'count']
        df.to_csv(self.output_file)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('collection')
    parser.add_argument('--input_layer')
    parser.add_argument('--output_file')
    parser.add_argument('--lemmas')
    args = vars(parser.parse_args())
    entity_counting = EntityCounting(collection=args['collection'],input_layer_name=args['input_layer'] or None,
                                     output_file=args['output_file'] or None, lemmas=args['lemmas'] or None)
    entity_counting.create_csv()