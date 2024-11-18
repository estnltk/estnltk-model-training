#
#  Collects manual evaluation results and outputs evaluation statistics
#
import re
import sys
import os.path

vm_layer = 'morph_analysis_flat'

if __name__ == '__main__':
    if len(sys.argv) < 2:
        raise Exception('(!) Missing input argument: name of the file with manually checked results.')
    input_file = sys.argv[1]
    assert os.path.exists(input_file), f'(!) Missing or bad input file path: {input_file}'
    references = []
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
    all_results = dict()
    cur_results = []
    cur_suggestions = dict()
    with open(input_file, mode='r', encoding='utf-8') as in_f:
        for line in in_f:
            line_clean = line.rstrip()
            if line_clean.count('::') == 3:
                references.append(line_clean)
                # record previous results
                if cur_results:
                    if len(cur_results) == 1:
                        key_result = cur_results[0]
                        if not key_result.startswith('difficult to tell'):
                            if key_result == f'{vm_layer} correct':
                                if cur_suggestions.get(vm_layer) > 1:
                                    all_results.setdefault(f'only {key_result}, but ambiguous', 0)
                                    all_results[f'only {key_result}, but ambiguous'] += 1
                                else:
                                    all_results.setdefault(f'only {key_result} and unambiguous', 0)
                                    all_results[f'only {key_result} and unambiguous'] += 1
                            else:
                                all_results.setdefault(f'only {key_result}', 0)
                                all_results[f'only {key_result}'] += 1
                        else:
                            all_results.setdefault(f'{key_result}', 0)
                            all_results[f'{key_result}'] += 1
                    elif len(cur_results) == 2:
                        if all(['correct' in s for s in cur_results]):
                            # Determine whether Vabamorf was ambiguous
                            if cur_suggestions.get(vm_layer) > 1:
                                all_results.setdefault('both correct, but vabamorf ambiguous', 0)
                                all_results['both correct, but vabamorf ambiguous'] += 1
                            else:
                                all_results.setdefault('both correct and unambiguous', 0)
                                all_results['both correct and unambiguous'] += 1
                        elif all(['difficult to tell' in s for s in cur_results]):
                            all_results.setdefault('difficult to tell', 0)
                            all_results['difficult to tell'] += 1
                        else:
                            raise NotImplementedError(f'(!) Unexpected cur_results={cur_results}')
                # reset current results
                cur_results = []
                cur_suggestions = dict()
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
            key_result = cur_results[0]
            if not key_result.startswith('difficult to tell'):
                if key_result == vm_layer:
                    if cur_suggestions.get(vm_layer) > 1:
                        all_results.setdefault(f'only {key_result}, but ambiguous', 0)
                        all_results[f'only {key_result}, but ambiguous'] += 1
                    else:
                        all_results.setdefault(f'only {key_result} and unambiguous', 0)
                        all_results[f'only {key_result} and unambiguous'] += 1
                else:
                    all_results.setdefault(f'only {key_result}', 0)
                    all_results[f'only {key_result}'] += 1
            else:
                all_results.setdefault(f'{key_result}', 0)
                all_results[f'{key_result}'] += 1
        elif len(cur_results) == 2:
            if all(['correct' in s for s in cur_results]):
                # Determine whether Vabamorf was ambiguous
                if cur_suggestions.get(vm_layer) > 1:
                    all_results.setdefault('both correct, but vabamorf ambiguous', 0)
                    all_results['both correct, but vabamorf ambiguous'] += 1
                else:
                    all_results.setdefault('both correct and unambiguous', 0)
                    all_results['both correct and unambiguous'] += 1
            elif all(['difficult to tell' in s for s in cur_results]):
                all_results.setdefault('difficult to tell', 0)
                all_results['difficult to tell'] += 1
            else:
                raise NotImplementedError(f'(!) Unexpected cur_results={cur_results}')
    for k in sorted(all_results, key=all_results.get, reverse=True):
        res_str = f' {k:50} {all_results[k]}/{len(references)}  {all_results[k]/len(references)*100.0:.2f}%'
        print(res_str)
