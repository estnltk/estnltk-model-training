
# Events extraction

This workflow creates `events` collection and related layers to `texts` collection and resolves conflicts between events and analysis printouts.

**Requires:**
* existence of `<prefix>_texts` collection
* layer `printout_segments` in `<prefix>_texts` collection

**Output:**

1) `<prefix>_texts__event_tokens__layer`
2) `<prefix>_texts__event_tokens2__layer`
3) `<prefix>_texts__event_segments__layer`
4) `<prefix>_texts__event_headers__layer`
5) `<prefix>_events`
6) `<prefix>_events__structure`

## CreateEventTokens2

An import part to explain is [CreateEventTokens2](https://git.stacc.ee/project4/cda-data-cleaning/blob/master/cda_data_cleaning/fact_extraction/event_extraction/step02_create_events_collection/create_events_collection.py#L303). Analysis printout tables are a subset of events, meaning that event extraction also tags analysis information. This leads to some parts of texts being tagged twice. The aim of CreateEventTokens2 is to resolve those conflicts. 
If some span has been tagged by 
* both events tokens and printouts => do not add anything to `event_tokens2 layer`
* only by printouts => do not add anything to `event_tokens2 layer`
* only by events tokens => add spans to `event_tokens2 layer`

