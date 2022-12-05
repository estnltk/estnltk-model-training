#
#  UDMorphComparator:
#  * compares automatically converted UD morph annotations against gold standard UD annotations.
#  * records comparison statistics (matches, mismatches, mismatch types);
#  * creates formatted output about matching and mismatching annotations, and about statistics;
#  

from collections import OrderedDict

def format_annotation(ann, attributes, feats_order=None):
    '''Formats given annotation as a concise string.'''
    out_str = []
    for k in attributes:
        if k != 'feats':
            out_str.append( f'{k}={ann[k]}' )
        else:
            if ann[k] != None:
                out_str_feats = []
                cur_feats_order = list(ann[k].keys())
                if feats_order is not None and len(feats_order) > 0:
                    assert isinstance(feats_order, list)
                    cur_feats_order = feats_order
                for k2 in cur_feats_order:
                    if k2 in ann[k].keys():
                        out_str_feats.append( f'{k2}={ann[k][k2]}' )
                if feats_order is not None and len(feats_order) > 0:
                    # add missing feats
                    for k2 in ann[k].keys():
                        if k2 not in feats_order:
                            out_str_feats.append( f'{k2}={ann[k][k2]}' )
                if len(ann[k].keys()) == 0:
                    out_str_feats.append( f'feats: []' )
                else:
                    out_str_feats[0] = f'feats: {out_str_feats[0]}'
                out_str.append( '; '.join(out_str_feats) )
            else:
                out_str.append( f'feats: None' )
    return '; '.join(out_str)


class UDMorphComparator:
    '''
    Compares automatically converted UD morph annotations against gold standard UD annotations.
    Records comparison statistics (matches, mismatches, mismatch types).
    Creates formatted output about matching and mismatching annotations, and about statistics.
    '''

    def __init__(self, gold_ud_layer:str, auto_ud_layer:str, 
                       attributes=("id", "lemma", "upostag", "xpostag", "feats"), 
                       ignore_attributes=("xpostag", "id"), 
                       ignore_lemma_underscores:bool=True, 
                       show_all_mismatches:bool=False, 
                       show_mismatch_types:bool=False ):
        '''
        Initiates UDMorphComparator.
        
        Parameters
        ----------
        gold_ud_layer: str
            Name of the gold standard UD annotations layer.
        auto_ud_layer:str
            Name of the automatically created UD annotations layer.
        attributes: list
            UD attributes from gold_ud_layer which values will be 
            checked in the auto_ud_layer.
            Default: ("id", "lemma", "upostag", "xpostag", "feats")
        ignore_attributes: list
            Subset of checked attributes which mismatches will be ignored.
            Default: ("xpostag", "id")
        ignore_lemma_underscores:bool
            If symbols '_' and '=' are ignored while matching lemmas.
        show_all_mismatches:bool
            Output generation option: if True, then all annotations 
            are output for all words, including ambiguous annotations. 
            If False, then all annotations are output only for mismatching 
            words, and for matching words, only the matching annotation 
            is output and ambiguities will be skipped.
        show_mismatch_types:bool
            Output generation option: if True, then mismatching words will 
            have the mismatch type show for the closest nearly matching 
            annotation.
            This is useful for debugging and looking up mismatches appearing 
            in mismatch statistics.
        '''
        self.gold_ud_layer = gold_ud_layer
        self.auto_ud_layer = auto_ud_layer
        self.attributes = attributes
        self.show_all_mismatches = show_all_mismatches
        self.show_mismatch_types = show_mismatch_types
        self.ignore_attributes = ignore_attributes
        self.ignore_lemma_underscores = ignore_lemma_underscores
        self.words = 0
        self.amb_words = 0
        self.matching_annotations = 0
        self.mismatching_annotations = 0
        self.mismatching_types = {}
    
    def _sort_auto_annotations_by_matches(self, gold_ud_annotations, auto_ud_annotations):
        '''Sorts auto annotations by their closeness to the gold annotation. 
           Auto annotations with smallest number of attribute mismatches come first.
           On the progress, also records statistics about matches/mismatches.
        '''
        assert len(gold_ud_annotations) == 1
        gold_ann = gold_ud_annotations[0]
        auto_annotations_with_scores = []
        for auto_ann in auto_ud_annotations:
            score = 0
            mismatching_fields = []
            for attr in self.attributes:
                if attr in self.ignore_attributes:
                    # Ignore these attributes in matching
                    continue
                if attr == 'feats':
                    # if feats is empty or null on both annotations, consider it match
                    if (gold_ann[attr] is None or len(gold_ann[attr].keys())==0) and \
                       (auto_ann[attr] is None or len(auto_ann[attr].keys())==0):
                        score += 1
                        continue
                if isinstance(gold_ann[attr], (OrderedDict, dict)):
                    # matching feats
                    for attr2 in gold_ann[attr].keys():
                        if gold_ann[attr].get(attr2) == auto_ann[attr].get(attr2, None):
                            score += 1
                        else:
                            mismatching_fields.append('feats::'+attr2)
                    # redundant feats
                    for attr2 in auto_ann[attr].keys():
                        if attr2 not in gold_ann[attr].keys():
                            score -= 1
                            mismatching_fields.append('feats::'+attr2)
                else:
                    value_gold = gold_ann[attr]
                    value_auto = auto_ann[attr]
                    if attr == 'lemma' and self.ignore_lemma_underscores:
                        # Remove underscores before match
                        value_gold = (value_gold.replace('_', '')).replace('=', '')
                        value_auto = (value_auto.replace('_', '')).replace('=', '')
                    if value_gold == value_auto:
                        score += 1
                    else:
                        mismatching_fields.append(attr)
            auto_annotations_with_scores.append( (mismatching_fields==[], auto_ann, score, mismatching_fields) )
        auto_annotations_with_scores = \
            sorted(auto_annotations_with_scores, key=lambda x: x[2], reverse=True)
        # Record statistics and try to detect type of the first mismatch
        # Note: this is somewhat heuristical, but gives us idea about what went wrong
        mismatch_type = '<>'
        self.words += 1
        if len(auto_annotations_with_scores) > 1:
            self.amb_words += 1
        first_mismatch = True
        has_full_match = auto_annotations_with_scores[0][0]
        for (full_match, auto_ann, score, mismatching_fields) in auto_annotations_with_scores:
            if full_match:
                self.matching_annotations += 1
            else:
                self.mismatching_annotations += 1
                if not has_full_match and first_mismatch:
                    mismatching_fields_clear = [m.replace('feats::', '') for m in mismatching_fields]
                    mismatch_type = gold_ann["upostag"]+':'+(",".join(mismatching_fields_clear))
                    # Determine mismatch type
                    if 'feats::Foreign' in mismatching_fields:
                        mismatch_type = 'Foreign='+gold_ann['feats'].get('Foreign', '<missing_from_gold>')
                    elif 'feats::Style' in mismatching_fields:
                        mismatch_type = 'Style='+gold_ann['feats'].get('Style', '<missing_from_gold>')
                    elif 'feats::Typo' in mismatching_fields:
                        mismatch_type = 'Typo='+gold_ann['feats'].get('Typo', '<missing_from_gold>')
                    elif "lemma" in mismatching_fields:
                        mismatch_type = 'Lemma'
                    elif auto_ann["xpostag"] in ['N', 'O'] and "upostag" not in mismatching_fields:
                        mismatch_type = f'Number mismatching {",".join(mismatching_fields_clear)}'
                    elif auto_ann["xpostag"] == 'G':
                        mismatch_type = f'G mismatching {",".join(mismatching_fields_clear)}'
                    elif "upostag" in mismatching_fields:
                        mismatch_type = f'upostag={gold_ann["upostag"]} mismatching'
                    if mismatch_type not in self.mismatching_types:
                        self.mismatching_types[mismatch_type] = 0
                    self.mismatching_types[mismatch_type] += 1
                    first_mismatch = False
        if self.show_mismatch_types:
            first_mismatch = True
            new_auto_annotations_with_scores = []
            for (full_match, auto_ann, score, mismatching_fields) in auto_annotations_with_scores:
                if mismatch_type != '<>' and first_mismatch:
                    new_auto_annotations_with_scores.append( (full_match, auto_ann, score, mismatching_fields, mismatch_type) )
                    first_mismatch = False
                else:
                    new_auto_annotations_with_scores.append( (full_match, auto_ann, score, mismatching_fields, '') )
            auto_annotations_with_scores = new_auto_annotations_with_scores
        return auto_annotations_with_scores

    def get_match_status_string(self, gold_ud_annotations, auto_ud_annotations):
        '''
        Returns output string showing the word, gold annotation and matching/mismatching auto annotations.
        On the progress, also records statistics about matches/mismatches.
        '''
        assert len(gold_ud_annotations) == 1
        gold_ann = gold_ud_annotations[0]
        gold_ann_feats = list(gold_ann['feats'].keys()) if gold_ann['feats'] is not None else []
        auto_annotations_with_scores = \
            self._sort_auto_annotations_by_matches(gold_ud_annotations, auto_ud_annotations)
        has_match = auto_annotations_with_scores and auto_annotations_with_scores[0][0]
        out_str = [gold_ann.text]
        out_str.append('  '+format_annotation(gold_ann, attributes=self.attributes))
        for entry in auto_annotations_with_scores:
            full_match         = entry[0]
            auto_ann           = entry[1]
            score              = entry[2]
            mismatching_fields = entry[3]
            mismatch_type      = entry[4] if len(entry) > 4 else ''
            if has_match and not full_match:
                if not self.show_all_mismatches:
                    # If there was a match, then do not display mismatches anymore (no use)
                    continue
            formatted_auto = format_annotation(auto_ann, attributes=self.attributes, feats_order=gold_ann_feats)
            mismatch_type_out = '' if len(mismatch_type) == 0 else f'  <{mismatch_type!r}>'
            out_str.append( ('  (+) ' if full_match else f'  (-{len(mismatching_fields)}) ')+formatted_auto+mismatch_type_out )
        return '\n'.join(out_str)

    def get_total_stats_string(self, display_types=False):
        '''
        Returns output string with statistics about words with correct annotations and ambiguous annotations.
        '''
        out_str = []
        percentage = f'({(self.matching_annotations/self.words)*100.0:.2f}%)'
        out_str.append( f' Words with correct annotations:   {self.matching_annotations} / {self.words} {percentage}')
        percentage = f'({(self.amb_words/self.words)*100.0:.2f}%)'
        out_str.append( f' Words with ambiguous annotations: {self.amb_words} / {self.words} {percentage}')
        if display_types:
            out_str.append('\n')
            out_str.append(self.get_mismatch_types_stats_string())
            out_str.append('\n')
        return '\n'.join(out_str)

    def get_mismatch_types_stats_string(self):
        '''
        Returns output string with statistics of mismatch types and frequency sorted list of mismatch types.
        '''
        out_str = []
        mismatches = self.words - self.matching_annotations
        percentage = f'({(mismatches/self.words)*100.0:.2f}%)'
        out_str.append( f' Words with incorrect annotations:   {mismatches} / {self.words} {percentage}')
        out_str.append( f' Mismatches by types ({len(self.mismatching_types.keys())}):  ')
        total_mismatches = sum([ v for k, v in self.mismatching_types.items() ])
        for k in sorted(self.mismatching_types.keys(), key=self.mismatching_types.get, reverse=True):
            v = self.mismatching_types.get(k)
            percentage = f'({(v/total_mismatches)*100.0:.2f}%)'
            out_str.append( f'    {v} {percentage}  {k!r}')
        return '\n'.join(out_str)





class UDMorphCatCombinationsRecorder:
    '''
    Records all combinations of UD categories/values in annotations.
    Keeps track of annotations appearing in ambiguous contexts.
    Creates UD categories/values listings on demand.
    '''

    def __init__(self, attributes=("upostag", "feats")):
        self.attributes = attributes
        self.all_categories = {}
        self.amb_categories = {}

    def record_amb_categories(self, ud_annotations):
        for auto_ann in ud_annotations:
            attr_values = []
            for attr in self.attributes:
                if attr == 'feats':
                    # if feats is empty or null
                    if (auto_ann[attr] is None or len(auto_ann[attr].keys())==0):
                        attr_values.append('__')
                        continue
                if isinstance(auto_ann[attr], (OrderedDict, dict)):
                    for attr2 in sorted(auto_ann[attr].keys()):
                        attr_values.append( f'{attr2}={auto_ann[attr][attr2]}' )
                else:
                    attr_values.append( f'{auto_ann[attr]}' )
            key = '|'.join(attr_values)
            if key not in self.all_categories:
                self.all_categories[key] = 0
            self.all_categories[key] += 1
            if len(ud_annotations) > 1:
                # Categories appearing in ambiguous contexts
                if key not in self.amb_categories:
                    self.amb_categories[key] = 0
                self.amb_categories[key] += 1

    def get_all_cat_listing_str(self):
        out_str = []
        for key in sorted(self.all_categories.keys()):
            out_str.append(key)
        return '\n'.join(out_str)

    def get_amb_cat_listing_str(self):
        if len(self.amb_categories.keys()) == 0:
            return ''
        out_str = []
        for key in sorted(self.amb_categories.keys()):
            out_str.append(key)
        return '\n'.join(out_str)