from estnltk import Text

from cda_data_cleaning.fact_extraction.event_extraction.step01_analysis_printout_extraction.taggers.type1_start_end_tagger import Type1StartEndTagger
from cda_data_cleaning.fact_extraction.event_extraction.step01_analysis_printout_extraction.taggers.type2_start_end_tagger import Type2StartEndTagger
from cda_data_cleaning.fact_extraction.event_extraction.step01_analysis_printout_extraction.taggers.type3_start_end_tagger import Type3StartEndTagger
from cda_data_cleaning.fact_extraction.event_extraction.step01_analysis_printout_extraction.taggers.type4_start_end_tagger import Type4StartEndTagger
from cda_data_cleaning.fact_extraction.event_extraction.step01_analysis_printout_extraction.taggers.type5_start_end_tagger import Type5StartEndTagger
from cda_data_cleaning.fact_extraction.event_extraction.step01_analysis_printout_extraction.taggers.type6_start_end_tagger import Type6StartEndTagger
from cda_data_cleaning.fact_extraction.event_extraction.step01_analysis_printout_extraction.taggers.type7_start_end_tagger import Type7StartEndTagger
from cda_data_cleaning.fact_extraction.event_extraction.step01_analysis_printout_extraction.taggers.segments_tagger_all_types import SegmentsTagger

type1_tagger = Type1StartEndTagger()
type2_tagger = Type2StartEndTagger()
type3_tagger = Type3StartEndTagger()
type4_tagger = Type4StartEndTagger()
type5_tagger = Type5StartEndTagger()
type6_tagger = Type6StartEndTagger()
type7_tagger = Type7StartEndTagger()
seg_tagger = SegmentsTagger()

def remove_table_segments(text):
    t = Text(text)
    type1_tagger.tag(t)
    type2_tagger.tag(t)
    type3_tagger.tag(t)
    type4_tagger.tag(t)
    type5_tagger.tag(t)
    type6_tagger.tag(t)
    type7_tagger.tag(t)
    seg_tagger.tag(t)
    segs = []
    for table in t.printout_segments.spans:
        # do not add empty tables
        if not table.enclosing_text:
            continue

        segs.append(table.start)
        segs.append(table.end)

    if len(segs) > 0:
        segs.append(len(t.text))
        segs.insert(0, 0)
        text = ""
        for j in range(0, len(segs), 2):
            text += t.text[segs[j]:segs[ j +1]]
        return text
    return t.text