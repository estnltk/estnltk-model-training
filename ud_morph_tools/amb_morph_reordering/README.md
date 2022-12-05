## Reordering of ambiguous morphological analyses

_Problem:_ Even after disambiguation, some morphological analyses in Vabamorf's output remain ambiguous. These ambiguous analyses are not sorted by probabilities. So, a commonly used strategy -- picking the first analysis in case of ambiguity -- is not a good one on Vabamorf's plain output, as the first analysis may not be the most likely one.

_Solution:_ We analyse manually annotated corpus ([Estonian UD treebank](https://github.com/UniversalDependencies/UD_Estonian-EDT)) and collect frequencies of correct variants of ambiguous morphological analyses. As a result, we get lexicons that can be used for reordering ambiguous analyses (by likelihoods) with the help of `MorphAnalysisReorderer` (see [this tutorial](https://github.com/estnltk/estnltk/blob/main/tutorials/nlp_pipeline/B_morphology/03_morph_analysis_reordering.ipynb) for details). This repository contains the source for creating the lexicons and evaluating their reordering performance.

_Requirements:_ EstNLTK version 1.7.1+

## Processing steps

* [`01_convert_ud_corpus_to_vm.ipynb`](01_convert_ud_corpus_to_vm.ipynb) -- converts Estonian UD treebank's data from UD format to Vabamorf's format. Saves results as EstNLTK's JSON files into folder `'UD_converted'`;

* [`02_word_to_analyses_freq_lexicons.ipynb`](02_word_to_analyses_freq_lexicons.ipynb) -- re-annotates data with   EstNLTK's Vabamorf, finds matches between automatic analyses and gold standard ones, and creates `word_to_analyses` frequency lexicons. A `word_to_analyses` lexicon shows for each ambiguous word, how many times each of its analysis was a correct one (according to manual annotations). Evaluates different `word_to_analyses` frequency lexicons on the test set, and examines how much the reordeing improves chances of getting the first analysis as a correct one;    

* [`03_category_freq_lexicons.ipynb`](03_category_freq_lexicons.ipynb) -- re-annotates data with EstNLTK's Vabamorf, finds matches between automatic analyses and gold standard ones, and creates  `partofspeech` and `form` frequency lexicons. These lexicons show for each `partofspeech` and `form` category, how frequently the category appeared as a correct category of ambiguous words. Evaluates category frequency lexicons (in combination with the best `word_to_analyses` frequency lexicon) on the test set, and examines how much reordeings improve chances of getting the first analysis as a correct one;

---

_Note:_ This is repository has been relocated from: https://github.com/estnltk/ambiguous-morph-reordering