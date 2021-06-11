# medBERT - Representation Learning on Free Text Medical Data

Work based on Meelis Perli's MSc thesis (spring 2021), guided by Raivo Kolde 
and Sven Laur. Initially based on EGCUT's free text data + diagnoses + doctor's 
speciality. 

Will be further standardised and trained on HWISC data.

Thesis: https://comserv.cs.ut.ee/ati_thesis/datasheet.php?language=en


## Summary - Thesis Abstract

Over 99% of the clinical records in Estonia are digitized. This is a great 
resource for clinical research, however, much of this data cannot be easily 
used, because a lot of information is in the free text format.

In recent years deep learning models have revolutionized the natural 
language processing field, enabling faster and more accurate ways to 
perform various tasks including named entity recognition and text 
classifications. To facilitate the use of such methods on Estonian medical 
records, this thesis explores the methods for pre-training the BERT models 
on the notes from “Digilugu”. Three BERT models were pre-trained on these notes. 

Two of the models were pre-trained from scratch. One on only the clinical notes, 
the other also used the texts from the Estonian National Corpus 2017. 

The third model is an optimized version of the EstBERT, which is a previously 
pre-trained model. To show the utility of such models and compare the 
performance, all four models were fine-tuned and evaluated on three 
classification and one named entity recognition downstream tasks. 

The best performance was achieved with the model trained only on notes. 
The transfer learning approach used to optimize the EstBERT model on 
the clinical notes improved the pre-training speed and performance, 
but still had slightly worse performance than the best model pre-trained 
in this thesis.

