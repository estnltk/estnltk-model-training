## Bert-based morphological tagger

### Notebook descriptions

#### [`01_finetune_bert_morph_tagger.ipynb`](01_finetune_bert_morph_tagger.ipynb)
It contains processes necessary to produce the BERT model that is used in BertMorphTagger.

Data used training tagger model has been gathered from the [corpus](https://github.com/estnltk/eval_experiments_lrec_2020/blob/master/scripts_and_data/enc2017_selection_plain_texts_json.zip) that was used in evaluation experiments reported in LREC 2020 paper "EstNLTK 1.6: Remastered Estonian NLP Pipeline"[^1].

Corpus has roughly 10 million words and contains five text types: fiction, periodicals, science, blogs and forums, and Wikipedia. About 575 000 words (~115 500 for each text type) were used in model training and evaluation.

**Sections**:
* Collecting data and applying morphological analysis using VabamorfTagger to the gathered data
* Statistics
* Data splitting
  * Grouping data by text type
  * Splitting data into sets (train/validation, test)
* Training and evaluating the model

Evaluation results:
| Model    | Precision | Recall | F1 score |
|----------|-----------|--------|----------|
| Bert     | 0.9545    | 0.9534 | 0.9540   |

| Text type        | Precision | Recall | F1 score |
|------------------|-----------|--------|----------|
| Blogs and forums | 0.9740    | 0.9733 | 0.9737   |
| Fiction          | 0.9991    | 0.9990 | 0.9990   |
| Periodicals      | 0.9808    | 0.9802 | 0.9805   |
| Science          | 0.9678    | 0.9688 | 0.9683   |
| Wikipedia        | 0.9479    | 0.9472 | 0.9476   |

Wikipedia's results are the poorest because it includes the comments and suggestions part of a Wiki article.

#### [`02_eval_UD_Est-EDT_treebank.ipynb`](02_eval_UD_Est-EDT_treebank.ipynb)
It contains the BERT model and Vabamorf evaluation using [converted version of the Estonian Dependency Treebank (EDT)](https://github.com/UniversalDependencies/UD_Estonian-EDT). The corpus has been manually reviewed and reannotated, meaning it is unrelated to BertMorphTagger and VabamorfTagger.

**Sections**:
* Converting UD corpus to Vabamorf format
* Creating and preparing the dataset from the converted UD corpus
* Model evaluation on UD corpus
* Vabamorf evaluation on UD corpus

**Evaluation results**
| Model    | Precision | Recall | F1 score |
|----------|-----------|--------|----------|
| Bert     | 0.9315    | 0.9162 | 0.9187   |
| Vabamorf | 0.9194    | 0.9067 | 0.9082   |

\* Metrics are from weighted average.

#### [`03_compare_with_vabamorf.ipynb`](03_compare_with_vabamorf.ipynb)
It contains:
* BertMorphTagger and VabamorfTagger comparison using the about 3 million words of unused data (not used in model training or evaluation, completely separate) from the corpus in `01_finetune_bert_morph_tagger.ipynb`. 
* comparison results that are saved as a `.txt` file containing the summary and all individual differences.

**Sections**:
* Gathering unused data
* Creating comparison data
* Using BertMorphTagger for predictions

Results can be found in the [`manual_check`](./manual_check/) directory, where manual checking of the differences gathered in [`03_compare_with_vabamorf.ipynb`](03_compare_with_vabamorf.ipynb) using the version of the model trained additionally on the [converted version of the Estonian Dependency Treebank (EDT)](https://github.com/UniversalDependencies/UD_Estonian-EDT)  ([`04_train_on_UD_EST-EDT_treebank.ipynb`](#04_train_on_ud_est-edt_treebankipynb)).

#### [`04_train_on_UD_EST-EDT_treebank.ipynb`](04_train_on_UD_EST-EDT_treebank.ipynb)
It contains:
* BertMorphTagger training on the [converted version of the Estonian Dependency Treebank (EDT)](https://github.com/UniversalDependencies/UD_Estonian-EDT).
* evaluation results using the `evaluate-metric/poseval` metric.

**Sections**
* Gathering Data
* Model Training
* Vabamorf evaluation on UD corpus

**Evaluation results**
| Model    | Precision | Recall | F1 score |
|----------|-----------|--------|----------|
| Bert     | 0.9778    | 0.9765 | 0.9769   |
| Vabamorf | 0.9194    | 0.9067 | 0.9082   |

\* Metrics are from weighted average

#### [`05_muna_homonym.ipynb`](05_muna_homonym.ipynb)
It contains:
* BertMorphTagger training only on the word "muna" in the sentences.
* evaluation on the sentences and [converted version of the Estonian Dependency Treebank (EDT)](https://github.com/UniversalDependencies/UD_Estonian-EDT).

**Sections**
* Gathering Data
* Model training
* Tagger Evaluation
* Evaluation on UD treebank
* Results

**Evaluation results**

Predicting label to word "muna"

| Model       | Accuracy  | Precision | F1 score |
|-------------|-----------|-----------|----------|
| NER         | 0.8350    | 0.8350    | 0.9101   |
| NER_v2      | 0.9320    | 0.9320    | 0.9648   |
| NER_v2_muna | 0.9903    | 0.9903    | 0.9951   |



Evaluations on UD treebank

| Model             | Precision | Recall | F1 score |
|-------------------|-----------|--------|----------|
| Vabamorf          | 0.9194    | 0.9067 | 0.9082   |
| NER_v2            | 0.9778    | 0.9765 | 0.9769   |
| NER_v2_muna[^2]   | 0.9568    | 0.9466 | 0.9474   |
| NER_v2_muna[^3]   | 0.9559    | 0.9455 | 0.9464   |

\* Metrics are from weighted average

[^1]: Sven Laur, Siim Orasmaa, Dage Särg, Paul Tammo. "EstNLTK 1.6: Remastered Estonian NLP Pipeline". *Proceedings of The 12th Language Resources and Evaluation Conference*. European Language Resources Association: Marseille, France, May 2020, p. 7154-7162

[^2]: Completely ignoring non-"muna" tokens
[^3]: Including other tokens in the loss with a smaller weight
