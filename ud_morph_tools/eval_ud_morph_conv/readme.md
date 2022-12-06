## Evaluation of UDMorphConverter on Estonian UD corpora

This repository contains code for large scale testing and evaluating [UDMorphConverter](https://github.com/estnltk/estnltk/blob/devel_1.7/tutorials/nlp_pipeline/B_morphology/06_morph_analysis_with_ud_categories.ipynb) -- tagger that converts Vabamorf's morphological categories to UD ([Universal Dependencies](https://universaldependencies.org/guidelines.html)) morphological categories.
The evaluation is done on Estonian UD corpora: [Estonian Dependency Treebank](https://github.com/UniversalDependencies/UD_Estonian-EDT) and [Estonian Web Treebank](https://github.com/UniversalDependencies/UD_Estonian-EWT).

*Requirements:* EstNLTK version 1.7.2+ ( `UDMorphConverter` must be importable from `estnltk.taggers` )

## Processing steps:

* First, download evaluation corpora: [Estonian Dependency Treebank](https://github.com/UniversalDependencies/UD_Estonian-EDT) and [Estonian Web Treebank](https://github.com/UniversalDependencies/UD_Estonian-EWT). Unpack into directories `UD_Estonian-EDT-master` and `UD_Estonian-EWT-master`. 

* Convert Estonian UD corpora from CONLL format to Estnltk Text objects and save as json files:

	* `python  01_conv_ud_corpora_to_json.py  <config-file>` -- converts both corpora (EDT and EWT) from CONLL to EstNLTK json and adds morphological analyses as specified in the configuration. Example configurations:

		* `conf_eval_amb_morf_ud_v2_10.ini` -- adds morphological analyses via `VabamorfAnalyzer` (morphological analysis without disambiguation: maximum number of analyses per word);
		* `conf_eval_default_morf_ud_v2_10.ini` -- adds morphological analyses via `VabamorfTagger` (default morphological analysis: the number of analyses is reduced via disambiguation);
	
	Note: you can change settings of the conversion (such as names of the input/output directories) in the configuration INI files. 
    If you download new versions of Estonian UD corpora, it is advisable to create version specific configurations.
    Both example configurations assume UD corpus versions 2.10.

* Evaluate `UDMorphConverter` on json files:

	* `python  02_eval_ud_morph_conv.py  <config-file>` -- evaluates `UDMorphConverter` (imported from `estnltk.taggers`) on converting Vabamorf's annotations to UD morphological annotations, collects and outputs evaluation statistics and writes log files with detailed word-by-word evaluation results, following the specified configuration. Example configurations:

		* `conf_eval_amb_morf_ud_v2_10.ini` -- evaluates converting ambiguous Vabamorf's analyses to UD morphological annotations. Creates evaluation log files for each corpus subset:
			* `eval_train_ud_v2_10_amb_morf_log_<datetime_of_evaluation>.txt`
			* `eval_dev_ud_v2_10_amb_morf_log_<datetime_of_evaluation>.txt`
			* `eval_test_ud_v2_10_amb_morf_log_<datetime_of_evaluation>.txt`
	
		* `conf_eval_default_morf_ud_v2_10.ini` -- evaluates converting default Vabamorf's analyses to UD morphological annotations. Creates evaluation log files for each corpus subset:
			* `eval_train_ud_v2_10_default_morf_log_<datetime_of_evaluation>.txt`
			* `eval_dev_ud_v2_10_default_morf_log_<datetime_of_evaluation>.txt`
			* `eval_test_ud_v2_10_default_morf_log_<datetime_of_evaluation>.txt` 

    * Look into evaluation log files for detailed evaluation results (words with correct/incorrect annotations);
    
    * Run `pyhton 03_summarise_eval_log_stats.py` to summarise results from all log files in a concise way. This sorts logs and prints out only final statistics from each log file:
    
    	* **Words with correct annotations** -- the percentage of correctly converted words, including words obtaining ambiguous annotations;
    	* **Words with ambiguous annotations** -- the percentage of words remaining ambiguous after the conversion;  


	Note: you can change settings of the conversion (such as `UDMorphConverter`'s parameters `remove_connegatives` and 
`generate_num_cases` and corpus subsets (`train`, `dev`, `test`)) in the configuration INI files.