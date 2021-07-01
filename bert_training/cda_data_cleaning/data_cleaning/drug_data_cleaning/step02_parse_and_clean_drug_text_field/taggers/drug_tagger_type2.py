from estnltk.finite_grammar import Rule, Grammar
from estnltk.taggers import Tagger
from estnltk.taggers import GrammarParsingTagger


class DrugTaggerType2(Tagger):
    """
    Parses drug using drug grammar.

    For terminal nodes use nodes[i].text
    For non-terminal nodes use nodes[i].attributes['name_of_the attribute'] (e.g. nodes[2].attributes['dose_quantity'])
    """

    input_layers = ["grammar_tags"]
    conf_param = ["tagger"]

    # TODO: By syntax this function is part of the class which is not the intended meaning
    #       However the code somehow works. Hence I do not modify it

    def atc_code_decorator(nodes):
        """
        Example:
            1. 'ATC koodid: C01DA14' => atc_code = 'C01DA14'
            2. 'C01DA14'             => atc_code = 'C01DA14'
        """
        if len(nodes) == 2:
            atc_code = nodes[1].text
        else:
            atc_code = nodes[0].text

        return {
            "date": "",
            "drug_name": "",
            "atc_code": atc_code,
            "active_ingredient": "",
            "dose_quantity": "",
            "dose_quantity_unit": "",
            "rate_quantity": "",
            "rate_quantity_unit": "",
            "package_type": "",
            "recipe_code": "",
            "package_size": "",
        }

    # TODO: By syntax this function is part of the class which is not the intended meaning
    #       However the code somehow works. Hence I do not modify it
    def type_2_decorator(nodes):
        """
        Corresponding rules
            1. 'atc_code'

        Example:
            1. Väljastatud ravimite ATC koodid: C01DA14

        Output:
            1. atc_code = 'C01DA14'
        """
        # atc_codes = [node['atc_code'] for node in nodes[0].support]
        # atc_codes = [node.text for node in nodes[0].support]
        # atc_codes = nodes[0].attributes['atc_code']

        return {
            "date": "",
            "drug_name": "",
            "atc_code": nodes[0].attributes["atc_code"],
            "active_ingredient": "",
            "dose_quantity": "",
            "dose_quantity_unit": "",
            "rate_quantity": "",
            "rate_quantity_unit": "",
            "package_type": "",
            "recipe_code": "",
            "package_size": "",
            "text_type": 2,
        }

    rules = []

    ## type 2 ##
    # peab kindlasti olema suurema prioriteediga kui 1.3
    # rules.append(Rule('TYPE_2', 'atc_code', group = 'g0',
    #                 priority=-4, decorator = type_2_decorator))

    rules.append(Rule("TYPE_2", "atc_code", group="g0", priority=-4, decorator=type_2_decorator))

    ## atc_code ##
    # Väljastatud ravimite ATC koodid: C01DA14 B01AC04 C07AB02 C09BA09 N05BA12
    rules.append(Rule("atc_code", "ATC_SYM ATC_CODE", group="g0", priority=-3, decorator=atc_code_decorator))

    rules.append(Rule("atc_code", "ATC_CODE", group="g0", priority=-2, decorator=atc_code_decorator))

    grammar = Grammar(
        start_symbols=["TYPE_2"],
        rules=rules,
        depth_limit=5,
        legal_attributes=[
            "text_type",
            "value",
            "recipe_code",
            "date",
            "dose_rate",
            "atc_code",
            "drug_name",
            "ingr",
            "active_ingredient",
            "dose_quantity",
            "dose_quantity_unit",
            "rate_quantity",
            "rate_quantity_unit",
            "package_type",
            "package_size",
        ],
    )

    def __init__(self, output_layer="parsed_drug", input_layer="grammar_tags"):
        self.output_attributes = (
            "name",
            "date",
            "atc_code",
            "recipe_code",
            "drug_name",
            "active_ingredient",
            "dose_quantity",
            "dose_quantity_unit",
            "rate_quantity",
            "rate_quantity_unit",
            "package_type",
            "package_size",
            "text_type",
        )
        self.tagger = GrammarParsingTagger(
            output_layer=output_layer,
            layer_of_tokens=input_layer,
            attributes=self.output_attributes,
            grammar=self.grammar,
            output_nodes={"TYPE_2"},
            output_ambiguous=False,
        )
        self.input_layers = [input_layer]
        self.output_layer = output_layer

    def _make_layer(self, text, layers, status):
        return self.tagger.make_layer(text=text, layers=layers, status=status)
