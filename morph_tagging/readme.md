## Bert-based morphological tagger

### Notebook descriptions

#### [`01_finetune_bert_morph_tagger.ipynb`](./01_finetune_bert_morph_tagger.ipynb)
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
| Model         | Precision | Recall | F1 score |
|---------------|-----------|--------|----------|
| Bert_morph_v1 | 0.9545    | 0.9534 | 0.9540   |

| Text type        | Precision | Recall | F1 score |
|------------------|-----------|--------|----------|
| Blogs and forums | 0.9740    | 0.9733 | 0.9737   |
| Fiction          | 0.9991    | 0.9990 | 0.9990   |
| Periodicals      | 0.9808    | 0.9802 | 0.9805   |
| Science          | 0.9678    | 0.9688 | 0.9683   |
| Wikipedia        | 0.9479    | 0.9472 | 0.9476   |

Wikipedia's results are the poorest because it includes the comments and suggestions part of a Wiki article.

#### [`02_eval_UD_Est-EDT_treebank.ipynb`](./02_eval_UD_Est-EDT_treebank.ipynb)
It contains the BERT model and Vabamorf evaluation using [converted version of the Estonian Dependency Treebank (EDT)](https://github.com/UniversalDependencies/UD_Estonian-EDT). The corpus has been manually reviewed and reannotated, meaning it is unrelated to BertMorphTagger and VabamorfTagger.

**Sections**:
* Converting UD corpus to Vabamorf format
* Creating and preparing the dataset from the converted UD corpus
* Model evaluation on UD corpus
* Vabamorf evaluation on UD corpus

**Evaluation results**
| Model         | Precision | Recall | F1 score |
|---------------|-----------|--------|----------|
| Bert_morph_v1 | 0.9315    | 0.9162 | 0.9187   |
| Vabamorf      | 0.9194    | 0.9067 | 0.9082   |

\* Metrics are from weighted average.

#### [`03_compare_with_vabamorf.ipynb`](./03_compare_with_vabamorf.ipynb)
It contains:
* BertMorphTagger and VabamorfTagger comparison using the about 3 million words of unused data (not used in model training or evaluation, completely separate) from the corpus in `01_finetune_bert_morph_tagger.ipynb`. 
* comparison results that are saved as a `.txt` file containing the summary and all individual differences.

**Sections**:
* Gathering unused data
* Creating comparison data
* Using BertMorphTagger for predictions

All annotations of both taggers and all differences can be found at: [diff_morph_texts_json_model_v1.zip](https://s3.hpc.ut.ee/estnltk/auxiliary-data/bert_morph_training/diff_morph_texts_json_model_v1.zip)

Manually checked 100 differences can be found in the directory [`manual_check`](./manual_check/)  (`01_morph_analysis_vs_bert_morph_x100_even_manual_check.txt` ). 
The direcory also contains file `02_morph_analysis_vs_bert_morph_x1000_even.txt` with 1000 randomly drawn differences that have not yet gone through manual evaluation. 

#### [`04_train_on_UD_EST-EDT_treebank.ipynb`](./04_train_on_UD_EST-EDT_treebank.ipynb)
It contains:
* BertMorphTagger training on the [converted version of the Estonian Dependency Treebank (EDT)](https://github.com/UniversalDependencies/UD_Estonian-EDT).
* evaluation results using the `evaluate-metric/poseval` metric.

**Sections**
* Gathering Data
* Model Training
* Vabamorf evaluation on UD corpus

**Evaluation results**
| Model         | Precision | Recall | F1 score |
|---------------|-----------|--------|----------|
| Bert_morph_v2 | 0.9778    | 0.9765 | 0.9769   |
| Vabamorf      | 0.9194    | 0.9067 | 0.9082   |

\* Metrics are from weighted average

#### [`04b_compare_model_2_with_vabamorf.ipynb`](./04b_compare_model_2_with_vabamorf.ipynb)
It contains code that generates comparison results saved as a `.txt` file where the summary and all individual differences are listed.

All annotations of both taggers and all differences can be found at: [diff_morph_texts_json_model_v2.zip](https://s3.hpc.ut.ee/estnltk/auxiliary-data/bert_morph_training/diff_morph_texts_json_model_v2.zip)

Manually checked approx 1000 differences can be found in the directory [`manual_check`](./manual_check/)  (`03_morph_analysis_vs_bert_morph_2_x1000_even_checked.txt` ).

#### [`05_muna_homonym.ipynb`](./05_muna_homonym.ipynb)
It contains:
* BertMorphTagger training only on the word "muna" in the [sentences](./experiment_with_homonym_muna/); 
* evaluation on the ["muna" sentences](./experiment_with_homonym_muna/) and [converted version of the Estonian Dependency Treebank (EDT)](https://github.com/UniversalDependencies/UD_Estonian-EDT).

**Sections**
* Gathering Data
* Model training
* Tagger Evaluation
* Evaluation on UD treebank
* Results

**Evaluation results**

Predicting label to word "muna"

| Model                    | Accuracy  | Precision | F1 score |
|--------------------------|-----------|-----------|----------|
| Vabamorf                 | 0.8350    | 0.8350    | 0.9101   |
| Bert_morph_v2            | 0.9320    | 0.9320    | 0.9648   |
| Bert_morph_v2_muna_1[^2] | 0.9903    | 0.9903    | 0.9951   |
| Bert_morph_v2_muna_2[^3] | 0.9903    | 0.9903    | 0.9951   |

Evaluations on UD treebank

| Model                    | Precision | Recall | F1 score |
|--------------------------|-----------|--------|----------|
| Vabamorf                 | 0.9194    | 0.9067 | 0.9082   |
| Bert_morph_v2            | 0.9778    | 0.9765 | 0.9769   |
| Bert_morph_v2_muna_1[^2] | 0.9568    | 0.9466 | 0.9474   |
| Bert_morph_v2_muna_2[^3] | 0.9559    | 0.9455 | 0.9464   |

\* Metrics are from weighted average

### Datasets and models

* Datasets used in these experiments can be found here: [finetuning_and_eval_data.zip](https://s3.hpc.ut.ee/estnltk/auxiliary-data/bert_morph_training/finetuning_and_eval_data.zip);
* Models are available in tartuNLP huggingface repository: [Bert_morph_v1](https://huggingface.co/tartuNLP/est-roberta-vm-morph-tagging/tree/b36e4e9ea1d1d0d3f2b4ec5e9f85b450ac53b1a2), [Bert_morph_v2](https://huggingface.co/tartuNLP/est-roberta-vm-morph-tagging/tree/a5c17c0f6f7eb88178d928bb8d2cfa35c6cdadf4)
    * We recommend to use models via [BertMorphTagger component in EstNLTK-neural](https://github.com/estnltk/estnltk/blob/f88269f2e1999d483a78a5c9a12226b3d6501f96/tutorials/nlp_pipeline/B_morphology/08_bert_based_morph_tagger.ipynb);



[^1]: Sven Laur, Siim Orasmaa, Dage Särg, Paul Tammo. "EstNLTK 1.6: Remastered Estonian NLP Pipeline". *Proceedings of The 12th Language Resources and Evaluation Conference*. European Language Resources Association: Marseille, France, May 2020, p. 7154-7162
[^2]: Bert_morph_v2 model trained on the finetuning phase, on the muna-training set completely ignoring non-"muna" tokens
[^3]: Bert_morph_v2 model trained on the finetuning phase, on the muna-training set including other tokens in the loss with a smaller weight
