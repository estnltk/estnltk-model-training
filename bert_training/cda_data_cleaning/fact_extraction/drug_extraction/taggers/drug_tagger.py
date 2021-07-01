from typing import Tuple

from estnltk.finite_grammar import Rule, Grammar
from estnltk.taggers import Tagger
from estnltk.taggers import MergeTagger, FlattenTagger
from estnltk.taggers import GrammarParsingTagger
from . import DrugCodeTagger
from . import DrugNameTagger
from . import DrugDateTagger


class DrugFieldPartTagger(Tagger):
    """
    Tags drug field parts.
    """

    conf_param = ["drug_name_tagger", "date_tagger", "drug_code_tagger", "flatten_tagger", "merge_tagger"]

    def __init__(
        self,
        output_attributes=("grammar_symbol", "value"),
        # conflict_resolving_strategy: str = 'MAX',
        # overlapped: bool = True,
        output_layer: str = "grammar_tags",
        # output_nodes={'ADDRESS'}
    ):
        self.input_layers = ["words"]
        self.output_attributes = tuple(output_attributes)
        self.output_layer = output_layer
        # priority_attribute = '_priority_'

        self.drug_name_tagger = DrugNameTagger()

        self.drug_code_tagger = DrugCodeTagger()

        self.date_tagger = DrugDateTagger()

        self.flatten_tagger = FlattenTagger(
            input_layer="drug_phrase",
            output_layer="drug_phrase_flat",
            output_attributes=["value", "_phrase_", "grammar_symbol"],
        )

        self.merge_tagger = MergeTagger(
            output_layer="grammar_tags",
            input_layers=["dates_numbers", "drug_phrase_flat", "drug_code"],
            output_attributes=["grammar_symbol", "value"],
        )

    def _make_layer(self, text, layers: dict, status):
        raw_text = text.text
        tmp_layers = layers.copy()
        tmp_layers["drug_phrase"] = self.drug_name_tagger.make_layer(text=text, layers=tmp_layers, status=status)
        tmp_layers["drug_code"] = self.drug_code_tagger.make_layer(text=text, layers=tmp_layers, status=status)
        tmp_layers["dates_numbers"] = self.date_tagger.make_layer(text=text, layers=tmp_layers, status=status)
        tmp_layers["drug_phrase_flat"] = self.flatten_tagger.make_layer(text=text, layers=tmp_layers, status=status)

        return self.merge_tagger.make_layer(text=text, layers=tmp_layers, status=status)


class DrugTagger(Tagger):
    """Parses drug using drug grammar."""

    input_layers = ["grammar_tags"]
    conf_param = ["tagger"]

    def date_decorator(nodes: Tuple):
        if len(nodes) == 5:
            return {
                "date": nodes[0].text,
                "drug_name": nodes[2].text,
                "atc_code": nodes[4].text,
                "active_ingredient": "",
            }
        else:
            return {"date": nodes[0].text, "drug_name": "", "atc_code": nodes[3].text, "active_ingredient": ""}

    def date_decorator2(nodes: Tuple):
        if len(nodes) == 7:
            return {
                "date": nodes[0].text,
                "drug_name": nodes[2].text,
                "atc_code": nodes[4].text,
                "active_ingredient": nodes[6].text,
            }
        else:
            return {
                "date": nodes[0].text,
                "drug_name": "",
                "atc_code": nodes[3].text,
                "active_ingredient": nodes[5].text,
            }

    def another_decorator(nodes: Tuple):
        drug_name = ""
        code = ""
        date = ""
        ingr = ""

        if len(nodes) == 1:

            code = nodes[0].text

        else:
            if nodes[0]["grammar_symbol"] == "DATE":
                date = nodes[0].text

            code = nodes[1].text

        return {"date": date, "drug_name": drug_name, "atc_code": code, "active_ingredient": ingr}

    def drug_name_decorator(nodes: Tuple):
        if len(nodes) == 1:
            return {"date": "", "drug_name": nodes[0].text, "atc_code": "", "active_ingredient": ""}
        else:
            return {"date": nodes[0].text, "drug_name": nodes[1].text, "atc_code": "", "active_ingredient": ""}

    def drug_code_decorator(nodes: Tuple):
        if len(nodes) == 1:
            return {"date": "", "drug_name": "", "atc_code": nodes[0].text, "active_ingredient": ""}
        else:
            return {"date": nodes[0].text, "drug_name": "", "atc_code": nodes[1].text, "active_ingredient": ""}

    def drug_ingr_decorator(nodes: Tuple):
        if len(nodes) == 1:
            return {"date": "", "drug_name": "", "atc_code": "", "active_ingredient": nodes[0].text}

        else:
            return {"date": nodes[0].text, "drug_name": "", "atc_code": "", "active_ingredient": nodes[1].text}

    rules = []

    rules.append(
        Rule(
            "DRUG",
            "DATE NAME drug_name CODE drug_code INGR drug_ingr",
            group="g0",
            priority=-3,
            decorator=date_decorator2,
        )
    )

    rules.append(
        Rule(
            "DRUG",
            "DATE NAME drug_name CODE drug_code INGR drug_name",
            group="g0",
            priority=-3,
            decorator=date_decorator2,
        )
    )

    rules.append(Rule("DRUG", "DATE NAME drug_name CODE drug_code", group="g0", priority=-2, decorator=date_decorator))

    rules.append(
        Rule("DRUG", "DATE NAME CODE drug_code INGR drug_ingr", group="g0", priority=-1, decorator=date_decorator2)
    )

    rules.append(
        Rule("DRUG", "DATE NAME CODE drug_code INGR drug_name", group="g0", priority=-1, decorator=date_decorator2)
    )

    rules.append(Rule("DRUG", "DATE NAME CODE drug_code", group="g0", priority=0, decorator=date_decorator))

    rules.append(
        Rule("DRUG1", "CODE drug_code", group="g0", priority=1, decorator=another_decorator, scoring=lambda x: 2)
    )

    rules.append(
        Rule("DRUG2", "drug_code", group="g0", priority=2, decorator=drug_code_decorator, scoring=lambda x: 1)
    )

    # rules.append(Rule('DRUG2', 'drug_code', group = 'g0',
    #                  priority=3, decorator = another_decorator, scoring=lambda x: 0))

    rules.append(Rule("DRUG", "drug_name", group="g0", priority=2, decorator=drug_name_decorator))

    # rules.append(Rule('DRUG', 'drug_name2', group = 'g0',
    #                  priority=8, decorator = another_decorator))

    # rules.append(Rule('DRUG', 'DATE drug_name2', group = 'g0',
    #                  priority=5, decorator = another_decorator))

    rules.append(Rule("DRUG", "DATE drug_ingr", group="g0", priority=2, decorator=drug_ingr_decorator))

    rules.append(Rule("DRUG", "drug_ingr", group="g0", priority=3, decorator=drug_ingr_decorator))

    rules.append(Rule("DRUG", "DATE drug_name", group="g0", priority=2, decorator=drug_name_decorator))

    grammar = Grammar(
        start_symbols=["DRUG", "DRUG1", "DRUG2", "DRUG3"],
        rules=rules,
        depth_limit=4,
        legal_attributes=["value", "date", "atc_code", "drug_name", "active_ingredient"],
    )

    def __init__(self, output_layer="parsed_drug", input_layer="grammar_tags"):
        self.output_attributes = ("name", "date", "atc_code", "drug_name", "active_ingredient")
        self.tagger = GrammarParsingTagger(
            output_layer=output_layer,
            layer_of_tokens=input_layer,
            attributes=self.output_attributes,
            grammar=self.grammar,
            output_nodes={"DRUG", "DRUG1", "DRUG2", "DRUG3"},
            output_ambiguous=False,
        )
        self.input_layers = [input_layer]
        self.output_layer = output_layer

    def _make_layer(self, text, layers, status):
        return self.tagger.make_layer(text=text, layers=layers, status=status)
