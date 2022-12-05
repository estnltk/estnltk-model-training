#
#   Utilities for:
#    *) Aligning EstNLTK's automatically produced morph_analysis 
#       annotations to gold standard annotations;
#    *) Collecting statistics about frequencies of ambiguous 
#       morph analyses;
#    *) Evaluating morph_analysis reorderer: which is the percentage 
#       of correct analyses at the first place after the reordering;
#

import csv, os, os.path
from enum import Enum
from collections import defaultdict

from estnltk.converters import json_to_text
from estnltk.default_resolver import DEFAULT_RESOLVER, make_resolver
from estnltk import Text, Layer, Annotation

# Switch off applying morph reorderer by default
RESOLVER_WITHOUT_REORDERER = make_resolver(use_reorderer=False)


class GoldStandard(Enum):
   UD_CORPUS  = 1                # https://github.com/UniversalDependencies/UD_Estonian-EDT
   MORF_KORPUS = 2               # (TODO) https://www.cl.ut.ee/korpused/morfkorpus/ 

# =========================================================================
#    Matching gold standard morph annotations to Vabamorf's 
#    ambiguous morph annotations
# =========================================================================

def find_vm_annotations_matching_gold_annotation( vm_annotations, gold_annotation, \
                                                  gold_standard_type=GoldStandard.UD_CORPUS, \
                                                  allow_partial_matches=True, \
                                                  add_ids=False ):
    '''Finds Vabamorf's annotations corresponding to:
          1) the (reduced) UD morph annotation (GoldStandard.UD_CORPUS);
          2) the gold standard vm annotation (GoldStandard.MORF_KORPUS);
       Returns a list with matching annotations.
    '''
    assert gold_standard_type in [GoldStandard.UD_CORPUS, GoldStandard.MORF_KORPUS], \
           '(!) Unexpected gold_standard_type={!r}'.format(gold_standard_type)
    if gold_standard_type == GoldStandard.UD_CORPUS:
        gold_lemma = gold_annotation['lemma'].replace('_','').replace('=','').replace('+','')
        gold_pos   = gold_annotation['pos']
        gold_form  = gold_annotation['form']
    elif gold_standard_type == GoldStandard.MORF_KORPUS:
        gold_lemma = gold_annotation['root'].replace('_','').replace('=','').replace('+','')
        gold_pos   = gold_annotation['partofspeech']
        gold_form  = gold_annotation['form']
    matching = []
    matching_vm_ids = []
    partial_matching = []
    partial_matching_vm_ids = []
    for aid, wm_annotation in enumerate(vm_annotations):
        # Check for lemma match
        cleaned_lemma = wm_annotation['root'].replace('_','').replace('=','').replace('+','')
        lemma_match = gold_lemma == cleaned_lemma
        # Check for pos match
        pos_match = wm_annotation['partofspeech'] == gold_pos
        # Check for form match
        form_match = wm_annotation['form'] == gold_form
        # Check for full match
        if lemma_match and pos_match and form_match:
            matching.append( wm_annotation )
            matching_vm_ids.append( aid )
        # Check for partial match
        if not lemma_match and pos_match and form_match:
            partial_matching.append( wm_annotation )
            partial_matching_vm_ids.append( aid )
    # If no lemma matches were found, but one partofspeech & form match was
    # found, assume that it is very likely the correct annotation ...
    if allow_partial_matches and len(matching)==0 and len(partial_matching)==1:
        return partial_matching if not add_ids else (partial_matching, partial_matching_vm_ids)
    return matching if not add_ids else (matching, matching_vm_ids)


def find_remaining_vm_annotatons( all_vm_annotations, excludable_annotations ):
    '''Filters all_vm_annotations: leaves out all annotations that are in the list excludable_annotations.'''
    included = []
    for aid, wm_annotation in enumerate(all_vm_annotations):
        match_found = False
        for excludable in excludable_annotations:
            if excludable == wm_annotation:
                match_found = True
                break
        if not match_found:
            included.append( wm_annotation )
    return included


def reduce_vm_annotations( vm_annotations, fields=['lemma','ending','clitic','partofspeech','form'], output_format='tuple' ):
    '''Rewrites vm annotation in a way that consist of only given fields.
       Returns list of annotations, each annotation in the format of 'tuple' (default) or 'dict'. '''
    assert output_format in ['tuple', 'dict'], '(!) Unexpected output format: {!r}'.format(output_format)
    assert len(fields) > 0
    reduced_annotations = []
    for vm_annotation in vm_annotations:
        if output_format == 'dict':
            ann_dict = {}
            for field in fields:
                ann_dict[field] = vm_annotation[field]
            reduced_annotations.append( ann_dict )
        elif output_format == 'tuple':
            ann_tuple = ()
            for field in fields:
                ann_tuple += (vm_annotation[field], )
            reduced_annotations.append( ann_tuple )
    return reduced_annotations


def collect_matches( loaded_texts, gold_morph_layer, \
                     gold_morph_type=GoldStandard.UD_CORPUS, \
                     focus_fields = ['lemma','ending','clitic','partofspeech','form'],
                     show_diff = False, show_warnings = False ):
    '''Collects word-to-correct-analyses statistics about ambiguous words.
       For each ambiguous word, makes a frequency list of its analyses based 
       on the manually corrected morph analysis in the gold_morph_layer.
       Prints general statistics about matching.
       Returns a dict mapping word's surface form to frequency list of its analyses.
    '''
    ambiguous = 0
    matches_found = 0
    words_total   = 0
    word_matches = {}
    seen_indistinguishable = set()
    for text in loaded_texts:
        words_total += len(text.morph_analysis)
        for wid, morph_word in enumerate(text.morph_analysis):
            if len(morph_word.annotations) > 1:
                ambiguous += 1
                gold_annotation = text[gold_morph_layer][wid].annotations[0]
                matching = find_vm_annotations_matching_gold_annotation( morph_word.annotations, gold_annotation, \
                                                                         gold_standard_type=gold_morph_type )
                if matching:
                    matches_found += 1
                    # Find remaining annotations
                    mismatching = find_remaining_vm_annotatons( morph_word.annotations, matching ) 
                    vm_matching_reduced    = reduce_vm_annotations( matching, fields=focus_fields, output_format='tuple' )
                    vm_mismatching_reduced = reduce_vm_annotations( mismatching, fields=focus_fields, output_format='tuple' )
                    is_propername = any([ar[2] for ar in vm_matching_reduced if ar[2] == 'H'])
                    word = morph_word.text.lower() if not is_propername else morph_word.text
                    #
                    # Note: after reducing annotations, some of them may become indistinguishable.
                    # Mostly these are proper names with different root tokenizations, such as:
                    #    ('Ekspress', '0', '', 'H', 'sg g')
                    #    ('Ekspress', '0', '', 'H', 'sg g')
                    # or
                    #    ('Hansapank', '0', '', 'H', 'sg n')
                    #    ('Hansapank', '0', '', 'H', 'sg n')
                    # Currently: Count these cases, and add an user warning about them.
                    # In future: find a fix, e.g. replace 'lemma' with 'root' in focus_fields.
                    #
                    if len(vm_matching_reduced) != len(set(vm_matching_reduced)) and \
                       word not in seen_indistinguishable:
                        anns_str_list = [ '  '+str(a) for a in vm_matching_reduced ]
                        if show_warnings:
                            print('(!) Warn: indistinguishable annotations: \n{}'.format( '\n'.join( anns_str_list )))
                        seen_indistinguishable.add( word )
                    elif len(vm_mismatching_reduced) != len(set(vm_mismatching_reduced)) and \
                         word not in seen_indistinguishable:
                        anns_str_list = [ '  '+str(a) for a in vm_mismatching_reduced ]
                        if show_warnings:
                            print('(!) Warn: indistinguishable annotations: \n{}'.format( '\n'.join( anns_str_list )))
                        seen_indistinguishable.add( word )
                    if word not in word_matches:
                        word_matches[word] = defaultdict(int)
                    for ann_reduced in vm_matching_reduced:
                        word_matches[word][ann_reduced] += 1
                    for ann_reduced in vm_mismatching_reduced:
                        word_matches[word][ann_reduced] += 0
                if show_diff and not matching:
                    morph_anns = [(a.root, a.partofspeech, a.form) for a in morph_word.annotations]
                    g_ann = (gold_annotation['root'], gold_annotation['partofspeech'], gold_annotation['form'])
                    print('  ',morph_anns)
                    print('  ',g_ann)
                    print()
    print(' Processed documents:                                ',len(loaded_texts) )
    percent = '({:.2f}%)'.format((ambiguous/words_total)*100.0)
    print(' Ambiguous words from total words:                   ',ambiguous, '/', words_total, percent)
    percent = '({:.2f}%)'.format((matches_found/ambiguous)*100.0)
    print(' Ambiguous words successfully matched to gold morph: ', matches_found, '/', ambiguous, percent)
    if len(seen_indistinguishable) > 0:
        percent = '({:.2f}%)'.format((len(seen_indistinguishable)/ambiguous)*100.0)
        print(' Ambiguous words with indistinguishable annotations: ', len(seen_indistinguishable), '/', ambiguous, percent)
    return word_matches


def collect_category_stats( loaded_texts, gold_morph_layer, \
                            gold_morph_type=GoldStandard.UD_CORPUS, \
                            collect_only_from_ambiguous = False, \
                            group_empty_form_with_postag = True, \
                            exclude_punctuation = True ):
    '''Collects part of speech and form category statistics from the corpus.
       For each word in the morph_analysis layer, finds the annotation that 
       matches the annotation on the gold_morph_layer, and collects
       part of speech and form category statistics from it.
       
       If collect_only_from_ambiguous is True, then collects statistics only
       from ambiguous words, otherwise from all words.
       
       If group_empty_form_with_postag is True, then the empty form is grouped 
       with part of speech.
       
       If exclude_punctuation == True, then statistics will not be collected 
       from punctuation tokens (part of speech tag = 'Z').
       
       Prints general statistics about matching.
       Returns two lists of tuples: part of speech ordering and form ordering;
    '''
    focus_fields = ['lemma','ending','clitic','partofspeech','form']
    ambiguous = 0
    matches_found = 0
    words_total   = 0
    postag_stats = defaultdict(int)
    form_stats   = defaultdict(int)
    for text in loaded_texts:
        for wid, morph_word in enumerate(text.morph_analysis):
            if len(morph_word.annotations) > 1:
                ambiguous += 1
            if collect_only_from_ambiguous and len(morph_word.annotations) == 1:
                # Skip unambiguous variants
                continue
            if exclude_punctuation and morph_word.annotations[0]['partofspeech'] == 'Z':
                # Skip punctuation
                continue
            gold_annotation = text[gold_morph_layer][wid].annotations[0]
            matching = find_vm_annotations_matching_gold_annotation( morph_word.annotations, \
                                                                     gold_annotation, \
                                                                     gold_standard_type=gold_morph_type )
            if matching:
                matches_found += 1
                # We can only collect statistics from vm annotations matching with the gold standard
                vm_matching_reduced = reduce_vm_annotations( matching, fields=focus_fields, output_format='dict' )
                for analysis in vm_matching_reduced:
                    postag_stats[analysis['partofspeech']] += 1
                    form = analysis['form']
                    if len(form) == 0 and group_empty_form_with_postag:
                        form = '_'+analysis['partofspeech']
                    elif len(form) == 0:
                        form = '_'
                    form_stats[form] += 1
            words_total += 1
    if collect_only_from_ambiguous:
        print(' Stats collected only from ambiguous words.')
    if exclude_punctuation:
        print(' Punctuation was excluded.')
    print(' Processed documents:                                ',len(loaded_texts) )
    percent = '({:.2f}%)'.format((ambiguous/words_total)*100.0)
    print(' Ambiguous words from total words:                   ',ambiguous, '/', words_total, percent)
    percent = '({:.2f}%)'.format((matches_found/words_total)*100.0)
    print(' Words successfully matched to gold morph:           ', matches_found, '/', words_total, percent)
    # Sort results and return tuples
    postag_tuples = [ (k, postag_stats[k]) for k in sorted(postag_stats.keys(), key=postag_stats.get, reverse=True) ]
    form_tuples   = [ (k, form_stats[k]) for k in sorted(form_stats.keys(), key=form_stats.get, reverse=True) ]
    return postag_tuples, form_tuples


def merge_collected_matches( word_matches_1, word_matches_2, focus_fields = ['lemma','ending','clitic','partofspeech','form'] ):
    '''Merges two sets of word_matches that were collected via collect_matches() function.
       Returns a dict mapping word's surface form to frequency list of its analyses.
    '''
    analyses_1 = 0
    analyses_2 = 0
    analyses_overlap = 0
    merged_matches = {}
    merged_analyses = 0
    for word in sorted(word_matches_1.keys()):
        merged_matches[word] = defaultdict(int)
        for match_1 in word_matches_1[word]:
            assert len(match_1) == len(focus_fields)
            merged_matches[word][match_1] += word_matches_1[word][match_1]
            merged_analyses += 1
            analyses_1 += 1
        if word in word_matches_2:
            for match_2 in word_matches_2[word]:
                assert len(match_2) == len(focus_fields)
                merged_matches[word][match_2] += word_matches_2[word][match_2]
                if match_2 in word_matches_1[word]:
                    analyses_overlap += 1
                else:
                    merged_analyses += 1
                analyses_2 += 1
    for word in sorted(word_matches_2.keys()):
        if word not in word_matches_1:
            assert word not in merged_matches
            merged_matches[word] = defaultdict(int)
            for match_2 in word_matches_2[word]:
                assert len(match_2) == len(focus_fields)
                merged_matches[word][match_2] += word_matches_2[word][match_2]
                merged_analyses += 1
                analyses_2 += 1
    print(' # words A:          ', len(word_matches_1.keys()))
    print(' # words B:          ', len(word_matches_2.keys()))
    common = set(word_matches_1.keys()).intersection( set(word_matches_2.keys()) )
    print(' # common words:     ', len( common ))
    print(' # merged words:     ', len( merged_matches.keys() ))
    print()
    print(' # analyses A:       ', analyses_1)
    print(' # analyses B:       ', analyses_2)
    print(' # analyses overlap: ', analyses_overlap)
    print(' # merged analyses:  ', merged_analyses)
    return merged_matches


def write_out_freq_sorted_annotations( outfname, items, fields = ['lemma','ending','clitic','partofspeech','form'], \
                                       freq_threshold=-1, add_probabilities=True, remove_ties=False, encoding='utf-8' ):
    '''Writes the freq sorted ambiguous analyses (items) into a tab-separated CSV file (outfname).'''
    output_csv = open(outfname, 'w', encoding=encoding, newline='')
    dialect='excel-tab'
    fmtparams={}
    csv_writer = csv.writer(output_csv, dialect=dialect, **fmtparams)
    if not add_probabilities:
        header_fields = ['text'] + fields + ['freq']
    else:
        header_fields = ['text'] + fields + ['prob', 'freq']
    csv_writer.writerow( header_fields )
    for keyword in sorted(items, key=lambda x: sum([items[x][k] for k in items[x].keys()]), reverse=True):
        total_freq = 0
        weights = []
        for annotation in sorted(items[keyword], key=items[keyword].get, reverse=True):
            total_freq += items[keyword][annotation]
            weights.append( items[keyword][annotation] )
        # Check if the frequency threshold is met
        if freq_threshold > -1 and freq_threshold>total_freq:
            continue
        if remove_ties:
            # Check for tie situations: do not add word in case of a tie
            ann_counts = [items[keyword][k] for k in items[keyword].keys()]
            if len(ann_counts) > 1 and len(set(ann_counts)) == 1:
                # A tie (all annotation counts were equal): skip the word
                continue
        # If required, calculate weights/probability estimations
        if add_probabilities and total_freq > 0:
            weights = [ round(w/total_freq, 4) for w in weights ]
        for aid, annotation in enumerate( sorted(items[keyword], key=items[keyword].get, reverse=True) ):
            freq = items[keyword][annotation]
            if not add_probabilities:
                full_entry = [keyword] + list(annotation) + [freq]
            else:
                full_entry = [keyword] + list(annotation) + [weights[aid], freq]
            assert len(full_entry) == len(header_fields)
            csv_writer.writerow( full_entry )
            #print('  ',ann, '  ',word_matches[word][ann])
    output_csv.close()


def write_out_freq_sorted_categories( outfname, items, field, freq_threshold=-1, encoding='utf-8' ):
    '''Writes the freq sorted categories (items) into a tab-separated CSV file (outfname).'''
    output_csv = open(outfname, 'w', encoding=encoding, newline='')
    dialect='excel-tab'
    fmtparams={}
    csv_writer = csv.writer(output_csv, dialect=dialect, **fmtparams)
    header_fields = [field, 'freq']
    csv_writer.writerow( header_fields )
    for (k, v) in sorted(items, key=lambda x : x[1], reverse=True):
        if freq_threshold > -1 and freq_threshold>v:
            continue
        csv_writer.writerow( [k, str(v)] )
    output_csv.close()


# =========================================================================
#    Evaluate the performance of Vabamorf's analyses reorderer
# =========================================================================

def evaluate_reorderer( morph_reorderer, input_dir, gold_morph_layer, gold_morph_type=GoldStandard.UD_CORPUS, exclude_strs=[], debug_take_first=False, show_fnames=True ):
    assert isinstance(exclude_strs, list)
    print('Loading evaluation texts ({})...'.format( gold_morph_type.name ))
    eval_texts = []
    for fname in os.listdir( input_dir ):
        exclude_file = False
        for excludable in exclude_strs:
            assert isinstance(excludable, str)
            if excludable in fname:
                exclude_file = True
                break
        if exclude_file:
            continue
        if fname.endswith('.json'):
            text = json_to_text(file=os.path.join(input_dir, fname) )
            if 'normalized_form' not in text.words.attributes:
                add_normalized_form_to_words( text.words )
            assert 'normalized_form' in text.words.attributes
            assert gold_morph_layer in text.layers
            # Add Vabamorf's default morph analysis without reordering
            text.tag_layer('morph_analysis', resolver=RESOLVER_WITHOUT_REORDERER)
            eval_texts.append( text )
            if show_fnames:
                print('  {!r} loaded and pre-annotated'.format(fname))
            if debug_take_first:
                break
    print(' Total {!r} texts loaded for evaluation. '.format(len(eval_texts)) )
    print()
    print(' Evaluation #1: Ambiguous analyses appear in their default ordering ')
    
    def evaluate_on_texts( input_texts, gold_morph_layer, gold_morph_type=GoldStandard.UD_CORPUS ):
        ambiguous = 0
        ambiguous_correct_first     = 0
        ambiguous_correct_not_first = 0
        ambiguous_correct_not_found = 0
        words_total = 0
        for text in input_texts:
            words_total += len(text.morph_analysis)
            for wid, morph_word in enumerate(text.morph_analysis):
                if len(morph_word.annotations) > 1:
                    ambiguous += 1
                    gold_annotation = text[gold_morph_layer][wid].annotations[0]
                    matching, vm_ids = \
                        find_vm_annotations_matching_gold_annotation( morph_word.annotations, gold_annotation, \
                                                                      gold_standard_type=gold_morph_type, add_ids=True )
                    if matching:
                        for anno, aid in zip(matching, vm_ids):
                            if aid == 0:
                                ambiguous_correct_first += 1
                            else:
                                ambiguous_correct_not_first += 1
                    else:
                        ambiguous_correct_not_found += 1
        print()
        print('  Ambiguous words total:          ',ambiguous)
        summary_snippet = ''
        if ambiguous > 0:
            percent = '({:.2f}%)'.format((ambiguous_correct_first/ambiguous)*100.0)
            print('   -- correct analysis first:     ',ambiguous_correct_first,'/',ambiguous, percent)
            percent = '({:.2f}%)'.format((ambiguous_correct_not_first/ambiguous)*100.0)
            print('   -- correct analysis not first: ',ambiguous_correct_not_first,'/',ambiguous, percent)
            percent = '({:.2f}%)'.format((ambiguous_correct_not_found/ambiguous)*100.0)
            print('   -- correct analysis not found: ',ambiguous_correct_not_found,'/',ambiguous, percent)
            print()
            summary_snippet = '{} / {} ({:.2f}%)'.format(ambiguous_correct_first, ambiguous, (ambiguous_correct_first/ambiguous)*100.0)
        return summary_snippet
        
    summary_1 = evaluate_on_texts( eval_texts, gold_morph_layer=gold_morph_layer, gold_morph_type=gold_morph_type )
    # Apply reorderer on texts
    for text in eval_texts:
        morph_reorderer.retag(text)
    print(' Evaluation #2: Ambiguous analyses have been reordered by the morph_reorderer')
    summary_2 = evaluate_on_texts( eval_texts, gold_morph_layer=gold_morph_layer, gold_morph_type=gold_morph_type )
    print()
    print(' Summary: correct analysis first: ', summary_1, '==>', summary_2)

def _copy_morph_layer( morph_layer ):
    copied_layer = Layer( name=morph_layer.name,
                          attributes=morph_layer.attributes,
                          text_object=morph_layer.text_object,
                          parent=morph_layer.parent,
                          enveloping=morph_layer.enveloping,
                          ambiguous=morph_layer.ambiguous,
                          default_values=morph_layer.default_values.copy() )
    for span in morph_layer:
        for annotation in span.annotations:
            annotation_dict = {attr:annotation[attr] for attr in morph_layer.attributes}
            copied_layer.add_annotation(span.base_span, annotation_dict)
    return copied_layer

def diff_reorderer( morph_reorderer_1, morph_reorderer_2, input_dir, gold_morph_layer, gold_morph_type=GoldStandard.UD_CORPUS, exclude_strs=[], debug_take_first=False, show_fnames=True, show_all_diffs=False ):
    assert isinstance(exclude_strs, list)
    assert morph_reorderer_1 is not None or morph_reorderer_2 is not None, '(!) No morph reorderer provided!'
    print('Loading evaluation texts ({})...'.format( gold_morph_type.name ))
    eval_texts = []
    for fname in os.listdir( input_dir ):
        exclude_file = False
        for excludable in exclude_strs:
            assert isinstance(excludable, str)
            if excludable in fname:
                exclude_file = True
                break
        if exclude_file:
            continue
        if fname.endswith('.json'):
            text = json_to_text(file=os.path.join(input_dir, fname) )
            if 'normalized_form' not in text.words.attributes:
                add_normalized_form_to_words( text.words )
            assert 'normalized_form' in text.words.attributes
            assert gold_morph_layer in text.layers
            # Add Vabamorf's default morph analysis without reordering
            text.tag_layer('morph_analysis', resolver=RESOLVER_WITHOUT_REORDERER)
            # Record the text
            eval_texts.append( text )
            if show_fnames:
                print('  {!r} loaded and pre-annotated'.format(fname))
            if debug_take_first:
                break
    print(' Total {!r} texts loaded for evaluation. '.format(len(eval_texts)) )
    print()
    # Apply reorderer on texts
    for text in eval_texts:
        # Two reorderers to compare
        if morph_reorderer_1 is not None and morph_reorderer_2 is not None:
            letters = ['a', 'b']
            for current_reorderer in [morph_reorderer_1, morph_reorderer_2]:
                # Make a backup copy of the default morph layer
                morph_copy = _copy_morph_layer( text['morph_analysis'] )
                morph_copy.text_object = None
                morph_copy.name = 'morph_analysis_old'
                text.add_layer( morph_copy )
                # Apply reorderer
                current_reorderer.retag(text)
                # Copy and rename the layer 
                morph_copy = _copy_morph_layer( text['morph_analysis'] )
                morph_copy.text_object = None
                morph_copy.name = 'morph_analysis_'+letters.pop(0)
                text.add_layer( morph_copy )
                text.pop_layer('morph_analysis')
                if len(letters) > 0:
                    # But back the old layer for the next reorderer
                    morph_copy = _copy_morph_layer( text['morph_analysis_old'] )
                    morph_copy.text_object = None
                    morph_copy.name = 'morph_analysis'
                    text.add_layer( morph_copy )
                text.pop_layer('morph_analysis_old')
        elif morph_reorderer_1 is None or morph_reorderer_2 is None:
            current_reorderer = morph_reorderer_1 if morph_reorderer_1 is not None else morph_reorderer_2
            # A single reorderer to compare with the default 
            # Make a backup copy of the default morph layer
            morph_copy = _copy_morph_layer( text['morph_analysis'] )
            morph_copy.text_object = None
            morph_copy.name = 'morph_analysis_a'
            text.add_layer( morph_copy )
            # Apply reorderer
            current_reorderer.retag(text)
            # Copy and rename the layer 
            morph_copy = _copy_morph_layer( text['morph_analysis'] )
            morph_copy.text_object = None
            morph_copy.name = 'morph_analysis_b'
            text.add_layer( morph_copy )
            text.pop_layer('morph_analysis')
        assert 'morph_analysis_a' in text.layers
        assert 'morph_analysis_b' in text.layers
        assert 'morph_analysis' not in text.layers
    # Evaluate and find differences
    if show_all_diffs:
        print('Showing differences for all words (including reocurring ones).')
    else:
        print('Showing differences for unique words (excluding reocurring words).')
    seen_differences  = set()
    total_differences  = 0
    total_improvements = 0
    for text in eval_texts:
        for wid, morph_word in enumerate(text.morph_analysis_b):
            old_morph_word = text.morph_analysis_a[wid]
            old_annotations = old_morph_word.annotations
            new_annotations = morph_word.annotations
            if old_annotations != new_annotations:
                total_differences += 1
                # Find out if the reordering improved the results
                gold_annotation = text[gold_morph_layer][wid].annotations[0]
                matching, vm_ids = \
                    find_vm_annotations_matching_gold_annotation( new_annotations, gold_annotation, \
                                                                  gold_standard_type=gold_morph_type, 
                                                                  add_ids=True )
                correct_is_first = len(matching) > 0 and vm_ids[0] == 0
                if morph_word.text.lower() not in seen_differences or show_all_diffs:
                    print( morph_word.text )
                    a_anns = [(o['lemma'], o['partofspeech'], o['form']) for o in old_annotations]
                    b_anns = [(n['lemma'], n['partofspeech'], n['form']) for n in new_annotations]
                    print('   ',a_anns)
                    print('    -->',b_anns, '(+)' if correct_is_first else '(-)')
                    seen_differences.add( morph_word.text.lower() )
                total_improvements += 1 if correct_is_first else 0
    print()
    print(' Total differences:  ', total_differences)
    if total_differences > 0:
        percent = '({:.2f}%)'.format((total_improvements/total_differences)*100.0)
        print(' Total improvements: ', total_improvements,'/',total_differences, percent)


# =========================================================================
#    Other helpful utils
# =========================================================================

def add_normalized_form_to_words( layer ):
    '''Rewrites words layer and add normalized_form attribute.'''
    layer.attributes += ('normalized_form',)
    for item in layer:
        item.clear_annotations()
        item.add_annotation( Annotation(item, normalized_form=None) )
    return layer


