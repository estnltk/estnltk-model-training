## Bert-based morphological tagger

### Notebook descriptions

#### [01_finetune_bert_morph_tagger.ipynb](01_finetune_bert_morph_tagger.ipynb)
Contains processes necessary to produce the BERT model that is used in BertMorphTagger.

Data used training tagger model has been gathered from the [corpus](https://github.com/estnltk/eval_experiments_lrec_2020/blob/master/scripts_and_data/enc2017_selection_plain_texts_json.zip) that was used in evaluation experiments reported in LREC 2020 paper "EstNLTK 1.6: Remastered Estonian NLP Pipeline"[^1].

Corpus has roughly 10 million words and contains 5 text types: fiction, periodicals, science, blogs and forums, and Wikipedia. About 575 000 words (~115 500 for each text type) were used in model training and evaluation.

**Sections**:
* Collecting data and applying morphological analysis using VabamorfTagger to the gathered data
* *Statistics*
* Data splitting
  * Grouping data by text type
  * Splitting data into sets (train/validation, test)
* Training and evaluating the model

Evaluation results:
| Model    | Precision | Recall | F1 score |
|----------|-----------|--------|----------|
| Bert     | 0.9545    | 0.9534 | 0.9540   |

| Text type        | Precision | Recall | F1 score |
|------------------|-----------|--------|----------|
| Blogs and forums | 0.9740    | 0.9733 | 0.9737   |
| Fiction          | 0.9991    | 0.9990 | 0.9990   |
| Periodicals      | 0.9808    | 0.9802 | 0.9805   |
| Science          | 0.9678    | 0.9688 | 0.9683   |
| Wikipedia        | 0.9479    | 0.9472 | 0.9476   |

Wikipedia's results are poorest because it includes the comments and suggestions part of a wiki article.

#### [02_eval_UD_Est-EDT_treebank.ipynb](02_eval_UD_Est-EDT_treebank.ipynb)
Contains BERT model and Vabamorf evaluation using [converted version of the Estonian Dependency Treebank (EDT)](https://github.com/UniversalDependencies/UD_Estonian-EDT). The corpus has been manually reviewed and reannotated, meaning it is not related to BertMorphTagger and VabamorfTagger.

**Sections**:
* Converting UD corpus to Vabamorf format
* Creating and preparing the dataset from the converted UD corpus
* Model evaluation on UD corpus
* Vabamorf evaluation on UD corpus

Evaluation results:
| Model    | Precision | Recall | F1 score |
|----------|-----------|--------|----------|
| Bert     | 0.9315    | 0.9162 | 0.9187   |
| Vabamorf | 0.9194    | 0.9067 | 0.9082   |

\* Metrics are from weighted average.

#### [03_compare_with_vabamorf.ipynb](03_compare_with_vabamorf.ipynb)
Contains BertMorphTagger and VabamorfTagger comparison using the about 3 million words of unused data (not used in model training or evaluation, completely separate) from the corpus in `01_finetune_bert_morph_tagger.ipynb`.

*Results will come soon.*

[^1]: Sven Laur, Siim Orasmaa, Dage Särg, Paul Tammo. "EstNLTK 1.6: Remastered Estonian NLP Pipeline". *Proceedings of The 12th Language Resources and Evaluation Conference*. European Language Resources Association: Marseille, France, May 2020, p. 7154-7162