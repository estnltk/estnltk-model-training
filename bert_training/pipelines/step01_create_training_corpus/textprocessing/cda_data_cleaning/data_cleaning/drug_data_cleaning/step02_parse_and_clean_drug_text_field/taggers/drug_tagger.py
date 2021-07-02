from estnltk.finite_grammar import Rule, Grammar
from estnltk.taggers import Tagger
from estnltk.taggers import GrammarParsingTagger


class DrugTagger(Tagger):
    """
    Parses drug using drug grammar.
    
    For terminal nodes use nodes[i].text
    For non-terminal nodes use nodes[i].attributes['name_of_the attribute'] (e.g. nodes[2].attributes['dose_quantity'])
    """

    input_layers = ["grammar_tags"]
    conf_param = ["tagger"]

    def __getitem__(self, k):
        return k

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

    def dose_decorator(nodes):
        """
        Example:
            1. '10 MG' => dose_quantity = '10', dose_quantity_unit = 'MG'
            2. '10'    => dose_quantity = '10'
        """
        dose_quantity_unit = ""

        if len(nodes) == 2:
            dose_quantity = nodes[0].text
            dose_quantity_unit = nodes[1].text
        else:
            dose_quantity = nodes[0].text

        return {
            "date": "",
            "drug_name": "",
            "atc_code": "",
            "active_ingredient": "",
            "dose_quantity": dose_quantity,
            "dose_quantity_unit": dose_quantity_unit,
            "rate_quantity": "",
            "rate_quantity_unit": "",
            "package_type": "",
            "recipe_code": "",
            "package_size": "",
        }

    def rate_decorator(nodes):
        """
        Example:
            1. '1 x pv' => rate_quantity = '1', rate_quantity_unit ='pv'
        """
        rate = nodes[0].text
        rate_quantity_unit = nodes[1].text

        return {
            "date": "",
            "drug_name": "",
            "atc_code": "",
            "active_ingredient": "",
            "dose_quantity": "",
            "dose_quantity_unit": "",
            "rate_quantity": rate,
            "rate_quantity_unit": rate_quantity_unit,
            "package_type": "",
            "recipe_code": "",
            "package_size": "",
        }

    def dose_rate_decorator(nodes):
        """
        Corresponding rules:
            1. dose rate
            2. NUMBER PACKAGE_TYPE RATE_QUANTITY_UNIT
            3. dose RATE_QUANTITY_UNIT

        Examples of the rules:
            1.  '1 ta 1 x pv' =>
                dose_quantity = '1',dose_quantity_unit='ta', rate_quantity='1', rate_quantity_unit='pv'
            2.  '1 kapsel päevas' =>
                dose_quantity = '1',dose_quantity_unit='kapsel', rate_quantity='1', rate_quantity_unit='päevas'
            3.1 '1 tbl päevas' =>
                dose_quantity = '1',dose_quantity_unit='tbl', rate_quantity='1', rate_quantity_unit='päevas'
            3.2 '1 päevas' =>
                dose_quantity = '1',dose_quantity_unit='', rate_quantity='1', rate_quantity_unit='päevas'
        """
        # rules 1. 3.
        if len(nodes) == 2:
            dose = nodes[0].attributes
            dose_quantity = dose["dose_quantity"]
            dose_quantity_unit = dose["dose_quantity_unit"]
        # rule 2
        else:
            dose_quantity = nodes[0].text
            dose_quantity_unit = nodes[1].text

        # rules 2. 3.
        if nodes[-1].name == "RATE_QUANTITY_UNIT":
            rate_quantity = "1"
            rate_quantity_unit = nodes[-1].text
        # rule 1
        else:
            rate = nodes[1].attributes
            rate_quantity = rate["rate_quantity"]
            rate_quantity_unit = rate["rate_quantity_unit"]

        return {
            "date": "",
            "drug_name": "",
            "atc_code": "",
            "active_ingredient": "",
            "dose_quantity": dose_quantity,
            "dose_quantity_unit": dose_quantity_unit,
            "rate_quantity": rate_quantity,
            "rate_quantity_unit": rate_quantity_unit,
            "package_type": "",
            "recipe_code": "",
            "package_size": "",
        }

    def date_decorator(nodes):
        """
        Example:
            1. 'Kuupäev: 20100728' => date = '20100728'
            2. '20100728'          => date = '20100728'
        """
        if len(nodes) == 2:
            # DATE_SYM DATE
            date = nodes[1].text
        else:
            date = nodes[0].text
        # else:  nodes[0].name == 'DATE':
        # MSEQ(DATE) element´
        # output dates: "12.01.2012, 13.02.2013"
        #    date = ', '.join([node.text for node in nodes[0].support])

        return {
            "date": date,
            "drug_name": "",
            "atc_code": "",
            "active_ingredient": "",
            "dose_quantity": "",
            "dose_quantity_unit": "",
            "rate_quantity": "",
            "rate_quantity_unit": "",
            "package_type": "",
            "recipe_code": "",
            "package_size": "",
        }

    def ingr_decorator(nodes):
        """
        Example:
            1. 'Toimeaine: metoprolool' => active_ingredient = 'metoprolool'
            2. 'metoprolool'            => active_ingredient = 'metoprolool'
        """
        if len(nodes) == 2:
            active_ingredient = nodes[1].text
        else:
            active_ingredient = nodes[0].text
        return {
            "date": "",
            "drug_name": "",
            "atc_code": "",
            "active_ingredient": active_ingredient,
            "dose_quantity": "",
            "dose_quantity_unit": "",
            "rate_quantity": "",
            "rate_quantity_unit": "",
            "package_type": "",
            "recipe_code": "",
            "package_size": "",
        }

    def recipe_code_decorator(nodes):
        """
        Example:
            1. '1010686329, 2010686329, 3010686329' => recipe_code = '1010686329, 2010686329, 3010686329'
            2. 'Rp.'                                => recipe_code = ''
                * useful for detecting the type of texts which start with 'Rp.' (Rp. IBUMETIN TBL 400MG N100)
        """

        if nodes[0].name == "MSEQ(RECIPE_CODE)":
            recipe_codes = [node.text for node in nodes[0].support]
        else:
            recipe_codes = ""

        return {
            "date": "",
            "drug_name": "",
            "atc_code": "",
            "active_ingredient": "",
            "dose_quantity": "",
            "dose_quantity_unit": "",
            "rate_quantity": "",
            "rate_quantity_unit": "",
            "package_type": "",
            "recipe_code": ", ".join(recipe_codes),
            "package_size": "",
        }

    def drug_name_decorator(nodes):
        """
        Example:
            1. 'Ravimi nimetus: ZOLADEX' => drug_name = 'ZOLADEX'
            2. 'ZOLADEX'                 => drug_name = 'ZOLADEX'
        """

        if len(nodes) == 2:
            drug_name = nodes[1].text  # [node.text for node in nodes[1].support]
        else:
            drug_name = nodes[0].text  # [node.text for node in nodes[0].support]

        return {
            "date": "",
            "drug_name": drug_name,  #', '.join(drug_names),
            "atc_code": "",
            "active_ingredient": "",
            "dose_quantity": "",
            "dose_quantity_unit": "",
            "rate_quantity": "",
            "rate_quantity_unit": "",
            "package_type": "",
            "recipe_code": "",
            "package_size": "",
        }

    def drug_name_dose_decorator(nodes):
        """
        Corresponding rules:
            1. 'drug_name PACKAGE_TYPE dose'
            2. 'drug_name DOSE_QUANTITY_UNIT dose'
            3. 'drug_name dose'
            4. 'drug_name'

        Examples of the rules:
           1. 'OMSAL CAPS 0,4 MG'
           2. 'DOXYCYCLIN  tablett  100mg'
           3.  OMEPRAZOL SANDOZ 20 MG'
           4. 'VALTREX'
        """

        # rule 4
        dose_quantity = ""
        dose_quantity_unit = ""
        package_type = ""
        drug_names = nodes[0].attributes["drug_name"]  # nodes[0]['drug_name']

        # rule 3
        if len(nodes) == 2:
            dose = nodes[1].attributes
            dose_quantity = dose["dose_quantity"]
            dose_quantity_unit = dose["dose_quantity_unit"]

        # rule 1. 2.
        if len(nodes) == 3:
            package_type = nodes[1].text
            dose = nodes[2].attributes
            dose_quantity = dose["dose_quantity"]
            dose_quantity_unit = dose["dose_quantity_unit"]

        return {
            "date": "",
            "drug_name": drug_names,
            "atc_code": "",
            "active_ingredient": "",
            "dose_quantity": dose_quantity,
            "dose_quantity_unit": dose_quantity_unit,
            "rate_quantity": "",
            "rate_quantity_unit": "",
            "package_type": package_type,
            "recipe_code": "",
            "package_size": "",
        }

    def drug_ingr_dose_decorator(nodes):
        """
        Corresponding rules:
           1. 'ingr PACKAGE_TYPE dose'
           2. 'ingr dose PACKAGE_TYPE'
           3. 'ingr PACKAGE_TYPE'
           4. 'ingr dose'
           5. 'ingr DOSE_QUANTITY_UNIT dose'

        Examples of the rules:
           1. 'simvastatiin õhukese polümeerikattega tablett 10 mg'
           2. 'metoprolool 50MG; toimeainet prolongeeritult vabastav tablett'
           3. 'metoprolool toimeainet prolongeeritult vabastav tablett'
           4. 'varfariin 3MG'
           5. 'hüdroklorotiasiid tablett 10 MG'
        """

        ingr = nodes[0]["active_ingredient"]
        package_type = ""
        dose_quantity = ""
        dose_quantity_unit = ""

        # corresponding to rules 1. and 3. and 5.
        if nodes[1].name == "PACKAGE_TYPE" or nodes[1].name == "DOSE_QUANTITY_UNIT":
            package_type = nodes[1].text
            if len(nodes) == 3:
                dose = nodes[-1].attributes
                dose_quantity = dose["dose_quantity"]
                dose_quantity_unit = dose["dose_quantity_unit"]

        # corresponding to rules 2. and 4.
        elif nodes[1].name == "dose":
            dose = nodes[1].attributes
            dose_quantity = dose["dose_quantity"]
            dose_quantity_unit = dose["dose_quantity_unit"]
            if len(nodes) == 3:
                package_type = nodes[2].text

        return {
            "date": "",
            "drug_name": "",
            "atc_code": "",
            "active_ingredient": ingr,
            "dose_quantity": dose_quantity,
            "dose_quantity_unit": dose_quantity_unit,
            "rate_quantity": "",
            "rate_quantity_unit": "",
            "package_type": package_type,
            "recipe_code": "",
            "package_size": "",
        }

    def type_1_decorator(nodes):
        """
        Corresponding rules:
            1. 'recipe_code date drug_ingr_dose dose_rate'
            2. 'recipe_code date drug_ingr_dose rate'
            3. 'recipe_code date drug_ingr_dose'
            4. 'recipe_code atc_code drug_ingr_dose rate',
            5. 'recipe_code atc_code drug_ingr_dose'

        Examples:
            1. '1018217714 20130405 telmisartaan+hüdroklorotiasiid tablett 80/12,5 mg 1 x pv'
            2. '1025393284 20140415 metoprolool toimeainet prolongeeritult vabastav tablett 1 ta 2 x pv'
            3. '1002965348 20101116 omeprasool gastroresistentne kõvakapsel'
            4. '1031878784 N06AB06- sertraliin õhukese polümeerikattega tablett 1 päevas'
            5. '1033706035 N06AX16- venlafaksiin toimeainet prolongeeritult vabastav kõvakapsel'

        Output:
            1. recipe_code = '1018217714', date = '20130405', atc_code = '', active_ingredient = 'telmisartaan+hüdroklorotiasiid', package_type = 'tablett', dose_quantity='80/12,5', dose_quantity_unit = 'mg',  rate_quantity='1', rate_quantity_unit = 'pv'
            2. recipe_code = '1025393284', date = '20140415', atc_code = '', active_ingredient = 'metoprolool', package_type = 'toimeainet prolongeeritult vabastav tablett', dose_quantity='1', dose_quantity_unit = 'ta',  rate_quantity='2', rate_quantity_unit = 'pv'
            3. recipe_code = '1002965348', date = '20101116', atc_code = '', active_ingredient = 'omeprasool', package_type = 'gastroresistentne kõvakapsel', dose_quantity='', dose_quantity_unit = '',  rate_quantity='', rate_quantity_unit = ''
            4. recipe_code = '1031878784', date = '', atc_code = 'N06AB06', active_ingredient = 'sertraliin', package_type = 'õhukese polümeerikattega tablett', dose_quantity='', dose_quantity_unit = '',  rate_quantity='1', rate_quantity_unit = 'päevas'
            5. recipe_code = '1033706035', date = '', atc_code = 'N06AX16', active_ingredient = 'venlafaksiin', package_type = 'toimeainet prolongeeritult vabastav kõvakapsel', dose_quantity='', dose_quantity_unit = '',  rate_quantity='', rate_quantity_unit = ''
        """

        date = ""
        atc_code = ""
        rate_quantity = ""
        rate_quantity_unit = ""
        dose_quantity = ""
        dose_quantity_unit = ""
        drug_ingr_dose = nodes[2].attributes

        # corresponding rule: 'recipe_code atc_code drug_ingr_dose'
        if nodes[1].name == "atc_code":
            atc_code = nodes[1]["atc_code"]
            dose_quantity = drug_ingr_dose["dose_quantity"]
            dose_quantity_unit = drug_ingr_dose["dose_quantity_unit"]

        # corresponding rule: 'recipe_code date drug_ingr_dose'
        if nodes[1].name == "date":
            date = nodes[1]["date"]
            dose_quantity = drug_ingr_dose["dose_quantity"]
            dose_quantity_unit = drug_ingr_dose["dose_quantity_unit"]

        # corresponding rule: 'recipe_code atc_code drug_ingr_dose rate'
        # corresponding rule: 'recipe_code date drug_ingr_dose rate'
        if nodes[-1].name == "rate":
            rate = nodes[3].attributes
            rate_quantity = rate["rate_quantity"]
            rate_quantity_unit = rate["rate_quantity_unit"]
            dose_quantity = drug_ingr_dose["dose_quantity"]
            dose_quantity_unit = drug_ingr_dose["dose_quantity_unit"]

        # corresponding rule: 'recipe_code date drug_ingr_dose dose_rate'
        if nodes[-1].name == "dose_rate":
            dose_rate = nodes[3].attributes
            rate_quantity = dose_rate["rate_quantity"]
            rate_quantity_unit = dose_rate["rate_quantity_unit"]
            dose_quantity = dose_rate["dose_quantity"]
            dose_quantity_unit = dose_rate["dose_quantity_unit"]

        return {
            "date": date,
            "drug_name": "",
            "atc_code": atc_code,
            "active_ingredient": drug_ingr_dose["active_ingredient"],
            "dose_quantity": dose_quantity,
            "dose_quantity_unit": dose_quantity_unit,
            "rate_quantity": rate_quantity,
            "rate_quantity_unit": rate_quantity_unit,
            "package_type": drug_ingr_dose["package_type"],
            "recipe_code": nodes[0]["recipe_code"],
            "package_size": "",
            "text_type": 1,
        }

    def type_2_decorator(nodes):
        """
        Corresponding rules
            1. 'atc_code'

        Example:
            1. Väljastatud ravimite ATC koodid: C01DA14

        Output:
            1. atc_code = 'C01DA14'
        """

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

    def type_3_decorator(nodes):
        """
        Corresponding rule:
            1. 'date recipe_code drug_name_dose SEQ(NUMBER) atc_code drug_ingr_dose dose_rate'
            2. 'date recipe_code drug_name_dose SEQ(NUMBER) atc_code drug_ingr_dose '

        Example:
            1. 'Väljastamise kuupäev: 26.11.2012, Retsepti number:  1015592331,
                Ravimi nimetus: AMANTADIN-RATIOPHARM  100 MG 100TK, Originaalide arv: 1,
                ATC kood: N04BB01, Toimeaine: Amantadinum,
                Ravimvorm: õhukese polümeerikattega tablett, Soodustus: 50%,
                Annustamine:  , Annustamine täpsemalt: 1 tablett päevas.'
            2. 'Väljastamise kuupäev: 02.02.2011, Retsepti number:  1003978133,
                Ravimi nimetus: ZOLADEX 1TK, Originaalide arv: 1,
                ATC kood: L02AE03, Toimeaine: Goserelinum,
                Ravimvorm: implantaat,Soodustus: 75%,
                Annustamine:  , Annustamine täpsemalt:  süstida s/c skeemi järgi.'

            Note: * dose under drug_ingr_dose is actually "Soodustus: 50%"
                  * drug dose can be either under 'drug_name_dose' or 'dose'

        Output:
            1. recipe_code = '1015592331', date = ' 26.11.2012,', atc_code = ' N04BB01,', drug_name = 'AMANTADIN-RATIOPHARM', active_ingredient = 'Amantadinum', dose_quantity = '1',  dose_quantity_unit = 'tablett',  rate_quantity = '1',  rate_quantity_unit = 'päevas', package_type = 'õhukese polümeerikattega tablett'
            2. recipe_code = '1003978133', date = ' 02.02.2011,', atc_code = ' L02AE03,', drug_name = 'ZOLADEX', active_ingredient = 'Goserelinum', dose_quantity = '1'
        """

        drug_name_dose = nodes[2].attributes
        drug_name = drug_name_dose["drug_name"]

        # rule 2.
        if len(nodes) == 6:
            dose_quantity = ""
            dose_quantity_unit = ""
            rate_quantity = ""
            rate_quantity_unit = ""

        # rule 1.
        else:
            # dose quantity under "Annustamine" or "Annustamine täpsemalt"
            dose_rate = nodes[6].attributes
            dose_quantity = dose_rate["dose_quantity"]
            dose_quantity_unit = dose_rate["dose_quantity_unit"]
            rate_quantity = dose_rate["rate_quantity"]
            rate_quantity_unit = dose_rate["rate_quantity_unit"]

        # search for dose elsewhere (in the rule it can be either under 'drug_name_dose' or 'dose')
        if dose_quantity == "":
            dose_quantity = drug_name_dose["dose_quantity"]
            dose_quantity_unit = drug_name_dose["dose_quantity_unit"]

        return {
            "date": nodes[0]["date"],
            "drug_name": drug_name,
            "atc_code": nodes[4]["atc_code"],
            "active_ingredient": nodes[5].attributes["active_ingredient"],
            "dose_quantity": dose_quantity,
            "dose_quantity_unit": dose_quantity_unit,
            "rate_quantity": rate_quantity,
            "rate_quantity_unit": rate_quantity_unit,
            "package_type": nodes[5].attributes["package_type"],
            "recipe_code": nodes[1]["recipe_code"],
            "package_size": "",
            "text_type": 3,
        }

    def type_4_decorator(nodes):
        """
        Corresponding rules:
            1. 'date drug_ingr_dose DOSE_QUANTITY_UNIT'
            2. 'date drug_ingr_dose'

        Examples:
            1. Kuupäev: 20120120, nimetus/toimeaine: klonasepaam 2 MG; tablett; 30 TK;
            2. Kuupäev: 20100728, nimetus/toimeaine: omeprasool 20MG; gastroresistentne kõvakapsel; 56 TK;

        Output:
            1. date = '20120120', active_ingredient = 'klonasepaam', dose_quantity = '2', dose_quantity = 'mg', package_type = 'tablett'
            2. date = '20100728', active_ingredient = 'omeprasool', dose_quantity = '20', dose_quantity = 'mg', package_type = 'gastroresistentne kõvakapsel'

        TODO:
            * unresolved are double/triple ingredients, right now only takes the first one
            * for example 'Kuupäev: 20140423, nimetus/toimeaine: tiamiin 50MG/1ML + püridoksiin 50MG/1ML + tsüanokobalamiin 0.5MG/1ML; + lidokaiin 10MG/1ML; 2 ML süstelahus; 10 TK;'
        """
        drug_ingr_dose = nodes[1].attributes

        # rule 1
        # package type can be under drug_ingr_dose or dose_quantity_unit (if it's called 'tablett')
        package_type = drug_ingr_dose["package_type"]
        if package_type == "" and len(nodes) == 3:
            package_type = nodes[2].text

        return {
            "date": nodes[0]["date"],
            "drug_name": "",
            "atc_code": "",
            "active_ingredient": drug_ingr_dose["active_ingredient"],
            "dose_quantity": drug_ingr_dose["dose_quantity"],
            "dose_quantity_unit": drug_ingr_dose["dose_quantity_unit"],
            "rate_quantity": "",
            "rate_quantity_unit": "",
            "package_type": package_type,
            "recipe_code": "",
            "package_size": "",
            "text_type": 4,
        }

    def type_5_decorator(nodes):
        """
        Corresponding rules:
            1. 'recipe_code date drug_name_dose'
            2. 'date drug_name_dose'

        Examples:
            1. '1017000739 05.02.2013 KLACID SR 500 MG', '1010686329, 2010686329, 3010686329 07.03.2012 ALLOPURINOL NYCOMED'
            2. '15.03.2015 OMEPRAZOL SANDOZ 20 MG', '18.06.2013 ZIBOR'

        Output:
            1. recipe_code = '1017000739', date = '05.02.2013 ', drug_name = 'KLACID SR', dose_quantity = '500', dose_quantity_unit = 'MG'
            2. recipe_code = '', date = '15.03.2015', drug_name = 'OMEPRAZOL SANDOZ', dose_quantity = '20', dose_quantity_unit = 'MG'
        """
        return {
            "date": nodes[-2]["date"],
            "drug_name": nodes[-1].attributes["drug_name"],
            "atc_code": "",
            "active_ingredient": "",
            "dose_quantity": nodes[-1].attributes["dose_quantity"],  # dose_quantity,#dose.attributes['dose_quantity'],
            "dose_quantity_unit": nodes[-1].attributes[
                "dose_quantity_unit"
            ],  # dose_quantity_unit,#dose.attributes['dose_quantity_unit'],
            "rate_quantity": "",
            "rate_quantity_unit": "",
            "package_type": nodes[-1].attributes["package_type"],
            "recipe_code": nodes[0]["recipe_code"],
            "package_size": "",
            "text_type": 5,
        }

    def type_6_decorator(nodes):
        """
        Corresponding rules:
            1. recipe_code drug_name_dose PACKAGE_SIZE
            2. recipe_code drug_name_dose

        Examples:
           1. Rp. RANIBERL TBL 150MG N50
           2. Rp. BIOPAROX AEROS 10ML

        Output:
            1. drug_name = 'RANIBERL', dose_quantity = '150', dose_quantity_unit = 'MG', package_size = 'N50'
            2. drug_name = 'BIOPAROX', dose_quantity = '10', dose_quantity_unit = 'ML'

       """
        drug_name_dose = nodes[1].attributes

        package_size = ""
        if len(nodes) == 3:
            package_size = nodes[2].text

        return {
            "date": "",
            "drug_name": drug_name_dose["drug_name"],
            "atc_code": "",
            "active_ingredient": "",
            "dose_quantity": drug_name_dose["dose_quantity"],
            "dose_quantity_unit": drug_name_dose["dose_quantity_unit"],
            "rate_quantity": "",
            "rate_quantity_unit": "",
            "package_type": drug_name_dose["package_type"],
            "recipe_code": nodes[0]["recipe_code"],
            "package_size": package_size,
            "text_type": 6,
        }

    def type_7_decorator(nodes):
        """
        Corresponding rules:
            1. 'ingr drug_name_dose PACKAGE_SIZE'
            2. 'drug_name_dose PACKAGE_SIZE'
            3. 'drug_ingr_dose PACKAGE_SIZE'

        Examples:
            1. Pentoxifyllinum, TRENTAL DEPOTBL 400 MG N100
            2. DOXYCYCLIN  tablett  100mg N10
            3. Amlodipinum TBL 5MG N60

        Output:
            1. active_ingredient = 'Pentoxifyllinum', drug_name = 'TRENTAL DEPOTBL', dose_quantity = '400', dose_quantity_unit = 'MG', package_size = 'N100'
            2. active_ingredient = '', drug_name = 'DOXYCYCLIN', dose_quantity = '100', dose_quantity_unit = 'mg', package_size = 'N10'
            3. active_ingredient = 'Amlodipinum', drug_name = '', dose_quantity = '5', dose_quantity_unit = 'MG', package_size = 'N60'
        """
        # rule 1.
        if len(nodes) == 3:
            drug_name_or_ingr_dose = nodes[1].attributes
            drug_ingr = nodes[0].attributes["active_ingredient"]
            drug_name = drug_name_or_ingr_dose["drug_name"]

        # rule 2.
        if nodes[0].name == "drug_name_dose":
            drug_name_or_ingr_dose = nodes[0].attributes
            drug_ingr = ""
            drug_name = drug_name_or_ingr_dose["drug_name"]

        # rule 3.
        if nodes[0].name == "drug_ingr_dose":
            drug_name_or_ingr_dose = nodes[0].attributes
            drug_ingr = drug_name_or_ingr_dose["active_ingredient"]
            drug_name = ""

        dose_quantity = drug_name_or_ingr_dose["dose_quantity"]
        dose_quantity_unit = drug_name_or_ingr_dose["dose_quantity_unit"]
        package_type = drug_name_or_ingr_dose["package_type"]

        return {
            "date": "",
            "drug_name": drug_name,
            "atc_code": "",
            "active_ingredient": drug_ingr,
            "dose_quantity": dose_quantity,
            "dose_quantity_unit": dose_quantity_unit,
            "rate_quantity": "",
            "rate_quantity_unit": "",
            "package_type": package_type,
            "recipe_code": "",
            "package_size": nodes[-1].text,
            "text_type": 7,
        }

    rules = []
    ## type 1.1 ##
    rules.append(
        Rule(
            "TYPE_1_1",
            "recipe_code date drug_ingr_dose dose_rate",
            group="g0",
            priority=-8,
            decorator=type_1_decorator,
            scoring=lambda x: 8,
        )
    )

    rules.append(
        Rule(
            "TYPE_1_1",
            "recipe_code date drug_ingr_dose rate",
            group="g0",
            priority=-7,
            decorator=type_1_decorator,
            scoring=lambda x: 7,
        )
    )

    rules.append(
        Rule(
            "TYPE_1_1",
            "recipe_code date drug_ingr_dose",
            group="g0",
            priority=-6,
            decorator=type_1_decorator,
            scoring=lambda x: 6,
        )
    )

    ## type 1.3
    rules.append(
        Rule(
            "TYPE_1_3",
            "recipe_code atc_code drug_ingr_dose rate",
            group="g0",
            priority=-7,
            decorator=type_1_decorator,
            scoring=lambda x: 7,
        )
    )

    rules.append(
        Rule(
            "TYPE_1_3",
            "recipe_code atc_code drug_ingr_dose",
            group="g0",
            priority=-6,
            decorator=type_1_decorator,
            scoring=lambda x: 6,
        )
    )
    # TEMPORARY
    # because in N06AB03-fluoksetiin can't extract fluoksetiin because they are written together
    # right now fluoksetiin will be excluded
    rules.append(
        Rule(
            "TYPE_1_3",
            "recipe_code atc_code PACKAGE_TYPE dose_rate",
            group="g0",
            priority=-5,
            decorator=type_1_decorator,
            scoring=lambda x: 6,
        )
    )

    ## type 2 ##
    # peab kindlasti olema suurema prioriteediga kui 1.3
    rules.append(Rule("TYPE_2", "atc_code", group="g0", priority=-4, decorator=type_2_decorator, scoring=lambda x: 4))

    ## type 3 ##
    rules.append(
        Rule(
            "TYPE_3",
            "date recipe_code drug_name_dose SEQ(NUMBER) atc_code drug_ingr_dose dose_rate",
            group="g0",
            priority=-8,
            decorator=type_3_decorator,
            scoring=lambda x: 8,
        )
    )

    rules.append(
        Rule(
            "TYPE_3",
            "date recipe_code drug_name_dose SEQ(NUMBER) atc_code drug_ingr_dose",
            group="g0",
            priority=-7,
            decorator=type_3_decorator,
            scoring=lambda x: 7,
        )
    )

    ## type 4 ##
    # peab kindlasti olema suurema prioriteediga kui 1.1, sest on selle alamhulk
    rules.append(
        Rule(
            "TYPE_4",
            "date drug_ingr_dose DOSE_QUANTITY_UNIT",
            group="g0",
            priority=-5,
            decorator=type_4_decorator,
            scoring=lambda x: 5,
        )
    )

    rules.append(
        Rule("TYPE_4", "date drug_ingr_dose", group="g0", priority=-4, decorator=type_4_decorator, scoring=lambda x: 4)
    )

    ## type 5.1 ##
    rules.append(
        Rule(
            "TYPE_5_1",
            "recipe_code date drug_name_dose",
            group="g0",
            priority=-7,
            decorator=type_5_decorator,
            scoring=lambda x: 7,
        )
    )

    ## type 5.2 ##
    rules.append(
        Rule(
            "TYPE_5_2", "date drug_name_dose", group="g0", priority=-6, decorator=type_5_decorator, scoring=lambda x: 6
        )
    )

    ## type 6 ##
    rules.append(
        Rule(
            "TYPE_6",
            "recipe_code drug_name_dose PACKAGE_SIZE",
            group="g0",
            priority=-4,
            decorator=type_6_decorator,
            scoring=lambda x: 4,
        )
    )

    rules.append(
        Rule(
            "TYPE_6",
            "recipe_code drug_name_dose",
            group="g0",
            priority=-3,
            decorator=type_6_decorator,
            scoring=lambda x: 3,
        )
    )

    ## type 7 ##
    rules.append(
        Rule(
            "TYPE_7",
            "ingr drug_name_dose PACKAGE_SIZE",
            group="g0",
            priority=-4,
            decorator=type_7_decorator,
            scoring=lambda x: 4,
        )
    )

    rules.append(
        Rule(
            "TYPE_7",
            "drug_name_dose PACKAGE_SIZE",
            group="g0",
            priority=-3,
            decorator=type_7_decorator,
            scoring=lambda x: 3,
        )
    )

    rules.append(
        Rule(
            "TYPE_7",
            "drug_ingr_dose PACKAGE_SIZE",
            group="g0",
            priority=-3,
            decorator=type_7_decorator,
            scoring=lambda x: 3,
        )
    )

    ## drug_name_dose ##
    # 'OMSAL CAPS 0,4 MG' or 'DOXYCYCLIN  tablett  100mg N10' or OMEPRAZOL SANDOZ 20 MG'  or 'Valaciclovirum, VALTREX'
    rules.append(
        Rule(
            "drug_name_dose",
            "drug_name PACKAGE_TYPE dose",
            group="g0",
            priority=-4,
            decorator=drug_name_dose_decorator,
            scoring=lambda x: 4,
        )
    )

    rules.append(
        Rule(
            "drug_name_dose",
            "drug_name DOSE_QUANTITY_UNIT dose",
            group="g0",
            priority=-4,
            decorator=drug_name_dose_decorator,
            scoring=lambda x: 4,
        )
    )

    rules.append(
        Rule(
            "drug_name_dose",
            "drug_name dose",
            group="g0",
            priority=-3,
            decorator=drug_name_dose_decorator,
            scoring=lambda x: 3,
        )
    )

    rules.append(
        Rule(
            "drug_name_dose",
            "drug_name",
            group="g0",
            priority=-2,
            decorator=drug_name_dose_decorator,
            scoring=lambda x: 2,
        )
    )

    ## drug_ingr_dose ##
    # VIST peab olema suurem prioriteediga, kui type 4
    rules.append(
        Rule(
            "drug_ingr_dose",
            "ingr PACKAGE_TYPE dose",
            group="g0",
            priority=-3,
            decorator=drug_ingr_dose_decorator,
            scoring=lambda x: 3,
        )
    )

    rules.append(
        Rule(
            "drug_ingr_dose",
            "ingr dose PACKAGE_TYPE",
            group="g0",
            priority=-3,
            decorator=drug_ingr_dose_decorator,
            scoring=lambda x: 3,
        )
    )

    rules.append(
        Rule(
            "drug_ingr_dose",
            "ingr PACKAGE_TYPE",
            group="g0",
            priority=-2,
            decorator=drug_ingr_dose_decorator,
            scoring=lambda x: 2,
        )
    )

    # ex 'Toimeaine: Ramiprilum, Ravimvorm: tablett, Soodustus: 75%'
    rules.append(
        Rule(
            "drug_ingr_dose",
            "ingr DOSE_QUANTITY_UNIT dose",
            group="g0",
            priority=-2,
            decorator=drug_ingr_dose_decorator,
            scoring=lambda x: 2,
        )
    )

    rules.append(
        Rule(
            "drug_ingr_dose",
            "ingr dose",
            group="g0",
            priority=-2,
            decorator=drug_ingr_dose_decorator,
            scoring=lambda x: 2,
        )
    )

    ## drug name ##
    # 'Ravimi nimetus: methadone'
    rules.append(Rule("drug_name", "DRUG_NAME_SYM DRUG_NAME", group="g0", priority=-1, decorator=drug_name_decorator))

    rules.append(Rule("drug_name", "DRUG_NAME", group="g0", priority=0, decorator=drug_name_decorator))

    rules.append(Rule("drug_name", "DRUG_NAME_SYM", group="g0", priority=0, decorator=drug_name_decorator))

    ## atc_code ##
    # Väljastatud ravimite ATC koodid: C01DA14 B01AC04 C07AB02 C09BA09 N05BA12
    rules.append(Rule("atc_code", "ATC_SYM ATC_CODE", group="g0", priority=-3, decorator=atc_code_decorator))

    rules.append(Rule("atc_code", "ATC_CODE", group="g0", priority=-2, decorator=atc_code_decorator))

    ## dose rate ##
    ## 1 ta 1 x pv
    rules.append(
        Rule("dose_rate", "dose rate", group="g0", priority=0, decorator=dose_rate_decorator, scoring=lambda x: 0)
    )

    # 1 tbl päevas, 1 päevas
    rules.append(
        Rule(
            "dose_rate",
            "dose RATE_QUANTITY_UNIT",
            group="g0",
            priority=1,
            decorator=dose_rate_decorator,
            scoring=lambda x: -1,
        )
    )

    #  1 kapsel päevas
    rules.append(
        Rule(
            "dose_rate",
            "NUMBER PACKAGE_TYPE RATE_QUANTITY_UNIT",
            group="g0",
            priority=2,
            decorator=dose_rate_decorator,
            scoring=lambda x: -2,
        )
    )

    ## dose ##
    # 10 MG, 10, 10MG, (10MG/1ML)??
    rules.append(
        Rule(
            "dose", "NUMBER DOSE_QUANTITY_UNIT", group="g0", priority=3, decorator=dose_decorator, scoring=lambda x: 0
        )
    )

    rules.append(Rule("dose", "NUMBER", group="g0", priority=5, decorator=dose_decorator, scoring=lambda x: -5))
    ## rate ##
    # 2 x pv
    # Note: pirority (4) must be between dose rule priorities (3 and 5)
    rules.append(Rule("rate", "NUMBER RATE_QUANTITY_UNIT", group="g0", priority=4, decorator=rate_decorator))

    ## date ##
    # "Kuupäev 12.10.2016" or "12.10.2016" or  "14.12.2016"
    rules.append(Rule("date", "DATE_SYM DATE", group="g0", priority=0, decorator=date_decorator, scoring=lambda x: 0))

    rules.append(Rule("date", "DATE", group="g0", priority=2, decorator=date_decorator, scoring=lambda x: 1))

    ## recipe ##
    # 'Rp' or '1025479379' or '1025479379 ' or '1010686368, 2010686368, 3010686368 '
    rules.append(
        Rule(
            "recipe_code", "RECIPE_SYM", group="g0", priority=-3, decorator=recipe_code_decorator, scoring=lambda x: 0
        )
    )

    rules.append(
        Rule(
            "recipe_code",
            "MSEQ(RECIPE_CODE)",
            group="g0",
            priority=-2,
            decorator=recipe_code_decorator,
            scoring=lambda x: 0,
        )
    )

    ## ingr ##
    # 'Toimeaine: medroksüprogesteroon' or 'medroksüprogesteroon'
    rules.append(Rule("ingr", "INGR_SYM INGR", group="g0", priority=0, decorator=ingr_decorator, scoring=lambda x: 0))

    rules.append(Rule("ingr", "INGR", group="g0", priority=1, decorator=ingr_decorator, scoring=lambda x: 0))

    grammar = Grammar(
        start_symbols=[
            "TYPE_1_1",
            "TYPE_1_3",
            "TYPE_2",
            "TYPE_3",
            "TYPE_4",
            "TYPE_5_1",
            "TYPE_5_2",
            "TYPE_6",
            "TYPE_7",
        ],
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
            output_nodes={
                "TYPE_1_1",
                "TYPE_1_3",
                "TYPE_2",
                "TYPE_3",
                "TYPE_4",
                "TYPE_5_1",
                "TYPE_5_2",
                "TYPE_6",
                "TYPE_7",
            },
            output_ambiguous=False,
        )
        self.input_layers = [input_layer]
        self.output_layer = output_layer

    def _make_layer(self, text, layers, status):
        return self.tagger.make_layer(text=text, layers=layers, status=status)
