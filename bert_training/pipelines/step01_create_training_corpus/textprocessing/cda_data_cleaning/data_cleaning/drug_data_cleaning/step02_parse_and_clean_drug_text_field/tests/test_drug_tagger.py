import re
import os
from estnltk.taggers import TaggerTester
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.data_cleaning.drug_data_cleaning import (
    DrugTagger,
)
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.data_cleaning.drug_data_cleaning import (
    DrugGrammarTokenTagger,
)


def run_tester(type_nr: int):
    """
    Runs tests in the folder corresponding to 'type_nr'.
    Tests are based  on all 'input_type*.json' and 'target_type*.json' files.
    """

    folder_name = "type_" + str(type_nr) + "/"

    path = "cda_data_cleaning/data_cleaning/drug_data_cleaning/step02_parse_and_clean_drug_text_field/tests/"

    # for file in os.listdir('./' + folder_name):
    for file in os.listdir(path + folder_name):
        # find input_*.json and target_*.json files
        if re.match("input_", file):
            target_file = file.replace("input", "target")

            tester = TaggerTester(
                tagger=drug_tagger, input_file=path + folder_name + file, target_file=path + folder_name + target_file
            )
            tester.load()
            tester.run_tests()


def test_type_1():
    run_tester(1)


def test_type_2():
    run_tester(2)


def test_type_3():
    run_tester(3)


def test_type_4():
    run_tester(4)


def test_type_5():
    run_tester(5)


def test_type_6():
    run_tester(6)


def test_type_7():
    run_tester(7)


drug_tagger = DrugTagger()
part_tagger = DrugGrammarTokenTagger()
