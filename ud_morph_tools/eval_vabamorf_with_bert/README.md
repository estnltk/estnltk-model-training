## VabamorfWithBertTagger evaluation on Estonian UD treebank 2.18


## Processing

EstNLTK version 1.7.4 is used.

- VabamorfWithBertTagger_UD_treebank_eval.ipynb

	1. converts UD treebank into vabamorf form and saves as json files. 
	2. Tags the treebank with VabamorfTagger and VabamorfWithBertTagger. 
	3. Calculates precision, recall and f1-score for taggers.

	There are 4 versions of this file:

		1. VabamorfWithBertTagger_UD_treebank_eval_v1 - original VabamorfWithBertTagger
		2. VabamorfWithBertTagger_UD_treebank_eval_v2 - original VabamorfWithBertTagger but vabamorf lemmas as altered
		3. VabamorfWithBertTagger_UD_treebank_eval_bertmorphtagger2_v1 - VabamorfWithBertTagger with new BertMorphTagger
		4. VabamorfWithBertTagger_UD_treebank_eval_bertmorphtagger2_v2 - VabamorfWithBertTagger with new BertMorphTagger and vabamorf lemmas are altered 

		The vabamorf lemmas are altered because ud_morph_reduced has verb lemmas without the -ma ending while vabamorf has -ma. 
		When vabamorf verb lemmas have the verb endings removed, then all the metrics also increase from 0.71 to 0.85.


| Tagger                   | BertMorphTagger |    verb lemmas   |    precision    |    recall    | f1-score  | 
|---------------| --------------- | ------------------------ | -------------------| -------------- | --------- |
|VabamorfTagger               |  original      | original   |  0.686790  |      0.686136       |   0.684519  | 
|VabamorfWithBertTagger|  original      | original   |   0.711642 |       0.713306      |   0.711546  |  
|VabamorfTagger                |  original      | endings removed  |  0.814965 |     0.817942    |  0.813721   |  
|VabamorfWithBertTagger|  original      | endings removed  | 0.850586  |   0.851079  |   0.849482  |  
|VabamorfTagger               |  modified    | original  |   0.686790 |    0.686136   |   0.684519  |  
|VabamorfWithBertTagger|  modified    | original  | 0.712092  |    0.713306   |  0.711785   | 
|VabamorfTagger               |  modified    | endings removed |  0.814965  |  0.817942  |   0.813721  |
|VabamorfWithBertTagger|  modified    | endings removed |  0.859616  |     0.860762        |   0.859187 |  

modified BertMorphTagger means that it includes verb analysis corrections.




- VabamorfWithBertTagger_UD_treebank_multiplicity.ipynb

	1. counts how many words have ambiguous morph analysis after VabamorfWithBertTagger.




### VabamorfWithBertTagger

- VabamorfWithBertTagger_tagging_problem.ipynb
	
	A problem with BertMorphTagger in VabamorfWithBertTagger.

	bert_morph_tagger2.py lines 329-332

	When creating a new morph layer with 
		>> morph_layer = self._bert_tokens_rewriter.make_layer(text, layers={morph_layer.name: morph_layer})
	
	then assert fails:
		>> AssertionError: ("Failed to rewrite 'morp_analysis' layer tokens to 'words' layer words: 909 != 910", "in the 'VabamorfWithBertTagger'")

	
	This problem is currently unsolved.


### bert_morph_tagger2.py 

- updated BertMorphTagger
	
- additions:

	- correct_verb_annotation=True  - leave vabamorf annaotation as is (False) or take Bert predicted pos to select the verb (True, applies only if there is no verb multiplicity) 

    - change_to_bert_form=True - if vabamorf form should remain as is (False) or change it to Bert predicted form for verb (True) that would include 'neg' (applies only if there is no verb multiplicity) 

updated code is mostly at lines 362-380





