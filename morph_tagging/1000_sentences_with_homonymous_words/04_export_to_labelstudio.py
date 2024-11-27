#
#   Converts files that have 1000 sentences with homonymous words to labelstudio annotation tasks. 
#   The goal is to disambiguate homonymous forms. 
#   Writes labelstudio import files into directory "to_labelstudio"
#
#   Requires estnltk version that has label_studio PhraseClassificationTask available
#   ( version 1.7.4+ )
# 

import os, os.path
import json

from estnltk import Text, Layer
from estnltk.converters.label_studio.labelling_configurations import PhraseClassificationConfiguration
from estnltk.converters.label_studio.labelling_tasks import PhraseClassificationTask

input_files = ['infl_type_01_randomly_picked_1000_sentences.jl',
               'infl_type_16_randomly_picked_1000_sentences.jl',
               'infl_type_17_randomly_picked_1000_sentences.jl',
               'infl_type_19_randomly_picked_1000_sentences.jl']

annotation_confs = [PhraseClassificationConfiguration(phrase_labels=['analüüsitav sõna'], 
                                                      class_labels={'sg n': 'sg n', 'sg g': 'sg g'}, 
                                                      header="Vali sõna morfoloogiline vorm (sg n - ainsuse nimetav, sg g -- ainsuse omastav):",
                                                      header_placement='middle'),
                    PhraseClassificationConfiguration(phrase_labels=['analüüsitav sõna'], 
                                                      class_labels={'sg n': 'sg n', 'sg g': 'sg g'}, 
                                                      header="Vali sõna morfoloogiline vorm (sg n - ainsuse nimetav, sg g -- ainsuse omastav):",
                                                      header_placement='middle'),
                    PhraseClassificationConfiguration(phrase_labels=['analüüsitav sõna'], 
                                                      class_labels={'sg n': 'sg n', 'sg g': 'sg g', 'sg p': 'sg p'}, 
                                                      header="Vali sõna morfoloogiline vorm (sg n - ainsuse nimetav, sg g - ainsuse omastav, sg p - ainsuse osastav):",
                                                      header_placement='middle'),
                    PhraseClassificationConfiguration(phrase_labels=['analüüsitav sõna'], 
                                                      class_labels={'sg g': 'sg g', 'sg p': 'sg p', 'adt': 'adt'}, 
                                                      header="Vali sõna morfoloogiline vorm (sg g - ainsuse omastav, sg p - ainsuse osastav, adt - lühike sisseütlev):",
                                                      header_placement='middle')]

assert len(input_files) == len(annotation_confs)

out_dir = 'to_labelstudio'
os.makedirs(out_dir, exist_ok=True)

for fid, fname in enumerate(input_files):
    annotation_conf = annotation_confs[fid]
    exported_data = []
    text_objs = []
    with open( fname, 'r', encoding='utf-8' ) as in_f:
        for line in in_f:
            line = line.strip()
            if len( line ) > 0:
                sentence_dict = json.loads(line)
                text = Text( sentence_dict['text'] )
                text.meta["corpus"] = sentence_dict["corpus"]
                text.meta["doc_id"] = sentence_dict["doc_id"]
                text.meta["sent_id"] = sentence_dict["sent_id"]
                layer = Layer('morph', attributes = ('label',), text_object=text)
                word_span = ( sentence_dict["start"], sentence_dict["end"] )
                assert (text.text[word_span[0]:word_span[1]]).lower() == sentence_dict["word"].lower(), sentence_dict["word"]
                layer.add_annotation( word_span, label='analüüsitav sõna' )
                text.add_layer(layer)
                text_objs.append(text) 
                #if len(text_objs) > 5:
                #    break
    task = PhraseClassificationTask(annotation_conf, input_layer='morph', 
                                                     output_layer='morph', label_attribute='label')
    exported_data = task.export_data(text_objs, indent=2)
    # Write out task configuration
    out_label_conf_fname = fname.replace('.jl', '_labeling_interface.txt')
    out_label_conf_fname = out_label_conf_fname.replace('_randomly_picked_', '_')
    with open(os.path.join(out_dir, out_label_conf_fname), 'w', encoding='utf-8') as out_f:
        out_f.write(task.interface_file)
    # Write out data for annotation task
    out_json_fname = fname.replace('.jl', '.json')
    out_json_fname = out_json_fname.replace('_randomly_picked_', '_')
    with open(os.path.join(out_dir, out_json_fname), 'w', encoding='utf-8') as out_f:
        out_f.write(exported_data)
