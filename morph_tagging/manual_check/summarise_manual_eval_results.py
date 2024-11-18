#
#  Collects manual evaluation results and outputs summarised evaluation statistics.
#
#  Outputs two types of statistics:
#
#    * Ambiguous tagging results, where Vabamorf's ambiguous output is still considered as 
#      correct if it contains a correct analysis among incorrect ones; 
#
#    * Disambiguation results, where only Bert's and Vabamorf's unambiguous and correct 
#      outputs are considered as correct results, and all other outputs are discarded as 
#      incorrect ones;
#
import re
import sys
import os.path
import warnings

vm_layer = 'morph_analysis_flat'

if __name__ == '__main__':
    if len(sys.argv) < 2:
        raise Exception('(!) Missing input argument: name of the file with manually checked results.')
    input_file = sys.argv[1]
    assert os.path.exists(input_file), f'(!) Missing or bad input file path: {input_file}'
    #
    # Examples of proposed analyses (without correct or uncertain tag)
    # morph_analysis_flat        ['A', '']
    # bert_morph_tagging_flat    ['A', '']
    # morph_analysis_flat        ['A', 'pl n']
    #
    pat_suggestion = re.compile(r'^(\S+)\s*(\[[^\[\]]+\])\s*$')
    #
    # Examples of correct matches:
    # morph_analysis_flat        ['V', 'tud'] (+)
    # bert_morph_tagging_flat    ['S', 'sg g'] (+)
    #
    pat_correct = re.compile(r'^(\S+)\s*(\[[^\[\]]+\])\s*\(\s*\+\s*\)\s*$')
    #
    # Examples of uncertain matches:
    # manual                     ['A', 'sg g']  (?)
    # bert_morph_tagging_flat    ['V', 'da'] (?)
    #
    pat_uncertain = re.compile(r'^(\S+)\s*(\[[^\[\]]+\])\s*\(\s*\?\s*\)\s*$')
    # Ambiguous tagging results (ambiguous output is also correct)
    all_results = dict()
    cur_results = []
    cur_suggestions = dict()
    # Disambiguation results (ambiguous output is considered incorrect)
    disamb_all_results = dict()
    all_differences = []
    annotated_differences = []
    with open( input_file, mode='r', encoding='utf-8' ) as in_f:
        for line in in_f:
            line_clean = line.rstrip()
            if line_clean.count('::') == 3:
                # Record previous results
                if len(cur_results) == 0:
                    if len(all_differences) > 0:
                        print(f'(!) Missing annotations for the difference {all_differences[-1]!r}')
                elif cur_results:
                    if len(cur_results) == 1:
                        # Only one is correct
                        key_result = cur_results[0]
                        if not key_result.startswith('difficult to tell'):
                            if key_result == f'{vm_layer} correct':
                                if cur_suggestions.get(vm_layer) > 1:
                                    # Vabamorf is correct, but ambiguous
                                    all_results.setdefault(f'only {key_result}, but ambiguous', 0)
                                    all_results[f'only {key_result}, but ambiguous'] += 1
                                    disamb_all_results.setdefault('both incorrect', 0)
                                    disamb_all_results['both incorrect'] += 1
                                else:
                                    # Vabamorf is correct and unambiguous
                                    all_results.setdefault(f'only {key_result} and unambiguous', 0)
                                    all_results[f'only {key_result} and unambiguous'] += 1
                                    disamb_all_results.setdefault(f'{key_result}', 0)
                                    disamb_all_results[f'{key_result}'] += 1
                            else:
                                # Only Bert or manual is correct
                                if f'{key_result}' != 'manual correct':
                                    all_results.setdefault(f'only {key_result}', 0)
                                    all_results[f'only {key_result}'] += 1
                                    disamb_all_results.setdefault(f'{key_result}', 0)
                                    disamb_all_results[f'{key_result}'] += 1
                                else:
                                    # manual correct  =>  both systems are incorrect
                                    all_results.setdefault(f'both incorrect ({key_result})', 0)
                                    all_results[f'both incorrect ({key_result})'] += 1
                                    disamb_all_results.setdefault('both incorrect', 0)
                                    disamb_all_results['both incorrect'] += 1
                        else:
                            # Difficult to tell
                            all_results.setdefault(f'{key_result}', 0)
                            all_results[f'{key_result}'] += 1
                            disamb_all_results.setdefault(f'{key_result}', 0)
                            disamb_all_results[f'{key_result}'] += 1
                    elif len(cur_results) >= 2:
                        if len(cur_results) > 2:
                            warnings.warn( f'(!) Multiple correct results annotated in {all_differences[-1]}' )
                        # Both are correct
                        if all(['correct' in s for s in cur_results]):
                            # Determine whether Vabamorf was ambiguous
                            if cur_suggestions.get(vm_layer) > 1:
                                all_results.setdefault('both correct, but vabamorf ambiguous', 0)
                                all_results['both correct, but vabamorf ambiguous'] += 1
                                for key_result in cur_results:
                                    if key_result != f'{vm_layer} correct':
                                        disamb_all_results.setdefault(f'{key_result}', 0)
                                        disamb_all_results[f'{key_result}'] += 1
                                        break
                            else:
                                all_results.setdefault('both correct and unambiguous', 0)
                                all_results['both correct and unambiguous'] += 1
                                recorded = set()
                                for key_result in cur_results:
                                    if f'{key_result}' not in recorded:
                                        disamb_all_results.setdefault(f'{key_result}', 0)
                                        disamb_all_results[f'{key_result}'] += 1
                                        recorded.add(f'{key_result}')
                        elif all(['difficult to tell' in s for s in cur_results]):
                            all_results.setdefault('difficult to tell', 0)
                            all_results['difficult to tell'] += 1
                            disamb_all_results.setdefault('difficult to tell', 0)
                            disamb_all_results['difficult to tell'] += 1
                        else:
                            raise NotImplementedError(f'(!) Unexpected cur_results={cur_results}')
                    # add new annotated difference
                    annotated_differences.append(all_differences[-1])
                # reset current results
                cur_results = []
                cur_suggestions = dict()
                # add new difference
                all_differences.append(line_clean)
            # Collect new results
            match_suggest = pat_suggestion.match(line_clean)
            if match_suggest:
                tagger = match_suggest.group(1)
                cur_suggestions.setdefault(tagger, 0)
                cur_suggestions[tagger] += 1
            match_corr = pat_correct.match(line_clean)
            if match_corr:
                tagger = match_corr.group(1)
                cur_results.append( f'{tagger} correct' )
                cur_suggestions.setdefault(tagger, 0)
                cur_suggestions[tagger] += 1
            match_uncertain = pat_uncertain.match(line_clean)
            if match_uncertain:
                tagger = match_uncertain.group(1)
                cur_results.append( 'difficult to tell' )
                cur_suggestions.setdefault(tagger, 0)
                cur_suggestions[tagger] += 1
    # record last results
    if cur_results:
        if len(cur_results) == 1:
            # Only one is correct
            key_result = cur_results[0]
            if not key_result.startswith('difficult to tell'):
                if key_result == f'{vm_layer} correct':
                    if cur_suggestions.get(vm_layer) > 1:
                        # Vabamorf is correct, but ambiguous
                        all_results.setdefault(f'only {key_result}, but ambiguous', 0)
                        all_results[f'only {key_result}, but ambiguous'] += 1
                        disamb_all_results.setdefault('both incorrect', 0)
                        disamb_all_results['both incorrect'] += 1
                    else:
                        # Vabamorf is correct and unambiguous
                        all_results.setdefault(f'only {key_result} and unambiguous', 0)
                        all_results[f'only {key_result} and unambiguous'] += 1
                        disamb_all_results.setdefault(f'{key_result}', 0)
                        disamb_all_results[f'{key_result}'] += 1
                else:
                    # Only Bert or manual is correct
                    if f'{key_result}' != 'manual correct':
                        all_results.setdefault(f'only {key_result}', 0)
                        all_results[f'only {key_result}'] += 1
                        disamb_all_results.setdefault(f'{key_result}', 0)
                        disamb_all_results[f'{key_result}'] += 1
                    else:
                        # manual correct  =>  both systems are incorrect
                        all_results.setdefault(f'both incorrect ({key_result})', 0)
                        all_results[f'both incorrect ({key_result})'] += 1
                        disamb_all_results.setdefault('both incorrect', 0)
                        disamb_all_results['both incorrect'] += 1
            else:
                # Difficult to tell
                all_results.setdefault(f'{key_result}', 0)
                all_results[f'{key_result}'] += 1
                disamb_all_results.setdefault(f'{key_result}', 0)
                disamb_all_results[f'{key_result}'] += 1
        elif len(cur_results) >= 2:
            if len(cur_results) > 2:
                warnings.warn( f'(!) Multiple correct results annotated in {all_differences[-1]}' )
            # Both are correct
            if all(['correct' in s for s in cur_results]):
                # Determine whether Vabamorf was ambiguous
                if cur_suggestions.get(vm_layer) > 1:
                    all_results.setdefault('both correct, but vabamorf ambiguous', 0)
                    all_results['both correct, but vabamorf ambiguous'] += 1
                    for key_result in cur_results:
                        if key_result != f'{vm_layer} correct':
                            disamb_all_results.setdefault(f'{key_result}', 0)
                            disamb_all_results[f'{key_result}'] += 1
                            break
                else:
                    all_results.setdefault('both correct and unambiguous', 0)
                    all_results['both correct and unambiguous'] += 1
                    recorded = set()
                    for key_result in cur_results:
                        if f'{key_result}' not in recorded:
                            disamb_all_results.setdefault(f'{key_result}', 0)
                            disamb_all_results[f'{key_result}'] += 1
                            recorded.add(f'{key_result}')
            elif all(['difficult to tell' in s for s in cur_results]):
                all_results.setdefault('difficult to tell', 0)
                all_results['difficult to tell'] += 1
                disamb_all_results.setdefault('difficult to tell', 0)
                disamb_all_results['difficult to tell'] += 1
            else:
                raise NotImplementedError(f'(!) Unexpected cur_results={cur_results}')
        # add new annotated difference
        annotated_differences.append(all_differences[-1])
    if len( annotated_differences ) > 0:
        if len( all_results.keys() ) > 0:
            print()
            print(f'Total differences annotated: {len(annotated_differences)} / {len(all_differences)}')
            print()
            checksum = 0
            print('Ambiguous tagging results (ambiguous output is still correct):')
            for k in sorted(all_results, key=all_results.get, reverse=True):
                res_str = f'  {k:50} {all_results[k]}/{len(annotated_differences)}  {all_results[k]/len(annotated_differences)*100.0:.2f}%'
                checksum += all_results[k]
                print(res_str)
            #print(checksum)
            print()
            checksum = 0
            print('Disambiguation results (ambiguous output is considered incorrect):')
            for k in sorted(disamb_all_results, key=disamb_all_results.get, reverse=True):
                res_str = f'  {k:50} {disamb_all_results[k]}/{len(annotated_differences)}  {disamb_all_results[k]/len(annotated_differences)*100.0:.2f}%'
                checksum += disamb_all_results[k]
                print(res_str)
            #print(checksum)
            print()
        else:
            print(f'(!) No manual markup detected from file {input_file}')
    else:
        print(f'(!) No references were detected from manual evaluation file {input_file}')
