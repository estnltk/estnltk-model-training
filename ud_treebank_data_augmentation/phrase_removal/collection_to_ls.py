from estnltk import Text
import random
import json
from typing import List


def conf_gen(deprel, classes: List[str]):
    single_label = '\t<Label value="{label_value}" background="{background_value}"/> \n'
    conf_string = """
<View>
    <Labels name="label" toName="text">\n
    """
    unnatural = """<Label value="$eval" background="#190FFD"/>\n"""
    if "obl" == deprel or deprel == "advmod":
        conf_string += unnatural
    end_block = """
</Labels>
<Text name="text" value="$text"/>
<Header value="Is the entity free, bound, incorrect or causes unnatural sentence?"/>
<Choices name="review" toName="text">
    <Choice value="free"/>
    <Choice value="bound"/>
    <Choice value="unnatural"/>
    <Choice value="incorrect"/>
    <Choice value="dubious"/>
    <Choice value="redundant comma"/>
    <Choice value="other redundant punctuation"/>
</Choices>
</View>"""

    for entry in classes:
        conf_string += single_label.format(
            label_value=entry,
            background_value=("#" + "%06x" % random.randint(0, 0xFFFFFF)).upper()
        )
    conf_string += end_block

    return conf_string


def obl_unnatural(text, span):
    unnat = "probably not unnatural"
    span_end = span.end
    found = False
    for word in text["words"]:
        if not found and word.start > span_end:
            following_deprel = text["conll_syntax"].get(word)["upostag"]
            if following_deprel.lower() == "verb":
                unnat = "possibly unnatural"
            break
    return unnat 

def advmod_unnatural(text, span):
    unnat = "probably not unnatural"
    found = False
    for word in text["words"]:
        if not found and word.start > span.end:
            following_deprel = text["conll_syntax"].get(word)["upostag"]
            if (following_deprel == "VERB" or following_deprel == "AUX") and span.start == 0:
                unnat = "possibly unnatural"
            break
    return unnat 


def one_text(text: Text, deprel, regular_layers: List[str], classification_layer: str = None, ner_layer: str = None):
    predictions = []
    results = {}
    score = None

    if classification_layer:
        if len(text[classification_layer]) > 0:
            print("üks")
            span = text[classification_layer][0]

            label = text[classification_layer][0]['label']
            score = text[classification_layer][0]['score']

            # Ignore spans without labels
            predictions.append({
                'to_name': "text",
                'from_name': "label",
                'type': 'labels',
                'value': {
                    "start": span.start,
                    "end": span.end,
                    "score": float(score),
                    "text": span.text,
                    "labels": [
                        str(classification_layer + "_" + label)
                    ]
                }
            })

    for_sure_piir = 0.75
    uncertain_piir = 0.25

    if ner_layer:
        if len(text[ner_layer]) > 0:
            for span in text[ner_layer]:
                score = span['score']

                suffix = None
                if score >= for_sure_piir:
                    suffix = "_" + str(for_sure_piir)
                elif uncertain_piir <= score < for_sure_piir:
                    suffix = "_" + str(uncertain_piir)
                if suffix:
                    predictions.append({

                        'to_name': "text",
                        'from_name': "label",
                        'type': 'labels',
                        'value': {
                            "start": span.start,
                            "end": span.end,
                            "score": float(score),
                            "text": span.text,
                            "labels": [
                                str(ner_layer + suffix)
                            ]
                        }
                    })




    for layer in regular_layers:
        if len(text[layer]) == 1:
            span = text[layer][0]
            
            if "obl" == deprel:
                 unnat = obl_unnatural(text, span)
            if deprel == "advmod":
                 unnat = advmod_unnatural(text, span)
 
            predictions.append({

                'to_name': "text",
                'from_name': "label",
                'type': 'labels',
                'value': {
                    "start": span.start,
                    "end": span.end,
                    "text": span.text,
                    "labels": [
                        str(layer)
                    ]
                }
            
                })
            data = {'text': text.text} 
            if "obl" == deprel or deprel == "advmod":
                data = {'text': text.text, "eval":unnat} 
            results = {
                'data': data,
                'predictions': [{
                    'result': predictions}
                    ]
            }

        elif len(text[layer]) > 1:
            results = []
            for i, span in enumerate(text[layer]):
            
                if "obl" == deprel:
                     unnat = obl_unnatural(text, span)
                if deprel == "advmod":
                     unnat = advmod_unnatural(text, span)
                
                predictions = [{
                    'to_name': "text",
                    'from_name': "label",
                    'type': 'labels',
                    'value': {
                        "start": span.start,
                        "end": span.end,
                        "text": span.text,
                        "labels": [
                            str(layer)
                        ]
                    }                 
                }]
                data = {'text': text.text} 
                if "obl" == deprel  or deprel == "advmod":
                    data = {'text': text.text, "eval":unnat} 
                results.append({
                    'data': data,
                    'predictions': [{
                        'result': predictions}
                        ]
                })
                

    # if score and classification_layer:
    #    results['predictions'][0]['score'] = float(score)

    return results


def collection_to_labelstudio(collection, 
                                deprel, 
                              regular_layers: List[str], filename: str,
                              classification_layer: str = None,
                              ner_layer: str = None
                              ):

    # Make sure imported layers exist in collection
    # assert something

    texts_list = [text for text in collection]

    data1 = [one_text(
        text, deprel,
        classification_layer=classification_layer,
        ner_layer=ner_layer,
        regular_layers=regular_layers) for text in texts_list]
    data = []
    
    for elem in data1:
        if type(elem) == list:
            for e in elem:
                data.append(e)
        else:
            data.append(elem)

    with open(filename, 'w') as f:
        json.dump(data, f)

