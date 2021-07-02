from estnltk import Text
from estnltk.converters import layer_to_dict

from cda_data_cleaning.fact_extraction.event_extraction.step02_create_events_collection.taggers import AnonymisedTagger


def test_anonymised_tagger():
    text = Text(
        'Nimi: <ANONYM id="0" type="per" morph="_H_ sg n"/>, '
        '<ANONYM id="1" type="per" morph="_H_ sg g"/> Uuringu kuupäev: 31.01.2014 '
        'Isikukood: <ANONYM id="2" type="ssn" /> '
        'registreerida telefoni teel: <ANONYM id="9" type="landline" />'
        'Arsti nimi <ANONYM id="33" type="per" morph="_H_ sg n;_H_ sg g,_H_ pl g"/> Arsti allkiri'
    )
    tagger = AnonymisedTagger()
    tagger.tag(text)
    assert layer_to_dict(text.anonymised) == {
        "ambiguous": True,
        "attributes": ("id", "type", "form", "partofspeech"),
        "enveloping": None,
        "meta": {},
        "name": "anonymised",
        "parent": None,
        "serialisation_module": None,
        "spans": [
            {"annotations": [{"form": "H", "id": "0", "partofspeech": "sg n", "type": "per"}], "base_span": (6, 50)},
            {"annotations": [{"form": "H", "id": "1", "partofspeech": "sg g", "type": "per"}], "base_span": (52, 96)},
            {"annotations": [{"form": None, "id": "2", "partofspeech": None, "type": "ssn"}], "base_span": (136, 164)},
            {
                "annotations": [{"form": None, "id": "9", "partofspeech": None, "type": "landline"}],
                "base_span": (194, 227),
            },
            {
                "annotations": [
                    {"form": "H", "id": "33", "partofspeech": "sg n", "type": "per"},
                    {"form": "H", "id": "33", "partofspeech": "sg g", "type": "per"},
                ],
                "base_span": (238, 301),
            },
        ],
    }
