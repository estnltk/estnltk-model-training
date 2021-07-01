from estnltk import Text
from estnltk.taggers import DateTagger

from cda_data_cleaning.common.preprocess_text import preprocess_text
from cda_data_cleaning.fact_extraction.event_extraction.step01_analysis_printout_extraction.taggers import \
    Type2StartEndTagger, Type1StartEndTagger, Type3StartEndTagger, Type4StartEndTagger, Type5StartEndTagger, \
    Type6StartEndTagger, Type7StartEndTagger, SegmentsTagger
from cda_data_cleaning.fact_extraction.event_extraction.step02_create_events_collection.taggers import \
    EventHeaderTagger, EventSegmentsTagger, EventTokenTagger, AnonymisedTagger
from cda_data_cleaning.fact_extraction.measurement_extraction.development.taggers import RobustDateNumberTagger
from pipelines.step01_create_training_corpus.textprocessing.span_merging import mergeSpans
from pipelines.step01_create_training_corpus.textprocessing.layer_modifications import getSpanRangesR, getSpanRangesF, \
    get_anon_replacement, get_number_replacement

# Table taggers
type1_tagger = Type1StartEndTagger()
type2_tagger = Type2StartEndTagger()
type3_tagger = Type3StartEndTagger()
type4_tagger = Type4StartEndTagger()
type5_tagger = Type5StartEndTagger()
type6_tagger = Type6StartEndTagger()
type7_tagger = Type7StartEndTagger()
seg_tagger = SegmentsTagger()

# Event taggers
event_token_tagger = EventTokenTagger(output_layer='event_tokens')
event_header_tagger = EventHeaderTagger()
event_segments_tagger = EventSegmentsTagger()

# Anon tagger
anon_tagger = AnonymisedTagger()

# Date and Number Tagger
date_num_tagger = RobustDateNumberTagger()


def get_table_spans(t, priority):
    type1_tagger.tag(t)
    type2_tagger.tag(t)
    type3_tagger.tag(t)
    type4_tagger.tag(t)
    type5_tagger.tag(t)
    type6_tagger.tag(t)
    type7_tagger.tag(t)
    seg_tagger.tag(t)
    return getSpanRangesR(t, "printout_segments", priority, "")


def get_event_header_spans(t, priority):
    t = event_token_tagger(t)
    t = event_header_tagger(t)
    t = event_segments_tagger(t)
    return getSpanRangesR(t, "event_headers", priority, "", textAsList=True)


def get_anonymised_spans(t, priority):
    t = anon_tagger(t)
    return getSpanRangesF(t, "anonymised", priority, get_anon_replacement)


def get_number_spans(t, priority):
    t = date_num_tagger(t)
    return getSpanRangesF(t, "dates_numbers", priority, get_number_replacement)


def preprocess_to_estnltk_Text(text):
    text = text.strip()
    text = preprocess_text(text)
    return Text(text)


def extract_span_ranges(t):
    spans = get_table_spans(t, 0)
    # spans = mergeSpans(spans, get_event_header_spans(t, 1))
    spans = mergeSpans(spans, get_anonymised_spans(t, 1))
    spans = mergeSpans(spans, get_number_spans(t, 2))
    return spans


def clean_text_with_spans(text, spans):
    new_text = ""
    start = 0
    for span in spans:
        if span["start"] == span["end"]:
            continue
        new_text += text[start:span["start"]]
        new_text += span["replaced_by"]
        start = span["end"]
    new_text += text[start:len(text)]
    return new_text


def clean(t):
    spans = extract_span_ranges(t)
    return clean_text_with_spans(t.text, spans)


def extract_sentences(t):
    sentences = []
    t.tag_layer(['tokens', 'words', 'sentences'])
    for sentence in t.sentences:
        sentences.append(" ".join(sentence.text))
    return sentences


def clean_and_extract_sentences(t):
    t.tag_layer(['tokens', 'words'])
    spans = extract_span_ranges(t)
    cleaned_text = clean_text_with_spans(t.text, spans)
    t_cleaned = preprocess_to_estnltk_Text(cleaned_text)
    return extract_sentences(t_cleaned)
