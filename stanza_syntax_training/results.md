## Evaluation results

* _Stanza version_ -- version of the stanza package used for training and evaluation of the models; 
* _Treebank_ -- version of the treebank used for training and evaluation; 
* _Morphosyntax_ -- which morphosyntactic preprocessing and tagset that was used as a basis for making syntactic predictions; 
* _Ensemble_ -- whether an ensemble of models was used (and if so, then how many models were used?)
* _Test LAS_ -- LAS (labelled attachment score) measured on the test set;
* _Train LAS_ -- LAS measured on the training set;
* _LAS GAP_ -- the difference between training and test LAS;

| Stanza version  |    Treebank              |    Morphosyntax    |    Ensemble    | Test LAS  | Train LAS | LAS gap | 
| --------------- | ------------------------ | -------------------| -------------- | --------- |---------- |---------|
|   1.4.2         | `UD_Estonian-EDT-r2.6`   | `morph_analysis`   |    No          |  0.8507   |  0.9231   | 0.0724  |
|   1.4.2         | `UD_Estonian-EDT-r2.6`   | `morph_extended`   |    No          |  0.8486   |  0.9176   | 0.0689  |
|   1.4.2         | `UD_Estonian-EDT-r2.6`   | `morph_extended`   |   10 models    |  0.8568   |  0.9337   | 0.0769  |
|   1.13.0        | `UD_Estonian-EDT-r2.18`  | `morph_analysis`   |    No          |  <span style="color:red">0.8299</span>   |  0.9043   | 0.0744  |
|   1.13.0        | `UD_Estonian-EDT-r2.18`  | `morph_extended`   |    No          |  <span style="color:red">0.8298</span>   |  0.9099   | 0.0801  |
|   1.13.0        | `UD_Estonian-EDT-r2.18`  | `morph_extended`   |   10 models    |  <span style="color:red">0.8352</span>   |  0.9203   | 0.0851  |

