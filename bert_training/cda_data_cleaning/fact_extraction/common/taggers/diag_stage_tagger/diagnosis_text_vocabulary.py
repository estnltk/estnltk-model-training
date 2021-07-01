from ..cancer_stage_tagger.cancer_stages_vocabulary import clean_stage

vocabulary = [
    {
        "grammar_symbol": "STAGE",
        "regex_type": "stage_1",
        "_regex_pattern_": r"(((I{0,1}V|I{1,3}|\d)[- ]*){1,2})([Ss]taadium|st\.?\s)",
        "_group_": 0,
        "_priority_": 0,
        "_validator_": lambda m: True,
        "value": lambda m: clean_stage(m.group(1).strip())
    },
    {
        "grammar_symbol": "STAGE",
        "regex_type": "stage_2",
        "_regex_pattern_": r"[Ss]taadium[-= ]*(I{0,1}V|I{1,3}|\d)",
        "_group_": 0,
        "_priority_": 0,
        "_validator_": lambda m: True,
        "value": lambda m: clean_stage(m.group(1).strip())
    },
    {
        "grammar_symbol": "RISK",
        "regex_type": "risk_1",
        "_regex_pattern_": r"(lisa)?risk(iaste)?[-= ]*(\d)",
        "_group_": 0,
        "_priority_": 0,
        "_validator_": lambda m: True,
        "value": lambda m: str(m.group(3).strip()).lower()
    },
    {
        "grammar_symbol": "RISK",
        "regex_type": "risk_2",
        "_regex_pattern_": r"(lisa)?risk(iaste)?[-= ]*(madal|mõõdukas|ülikõrge|väga kõrge|kõrge|keskmine)",
        "_group_": 0,
        "_priority_": 0,
        "_validator_": lambda m: True,
        "value": lambda m: str(m.group(3).strip()).lower()
    },
    {
        "grammar_symbol": "RISK",
        "regex_type": "risk_3",
        "_regex_pattern_": r"(madal|mõõdukas|ülikõrge|väga kõrge|kõrge|keskmine)[- ]*(lisa)?risk(iaste)?",
        "_group_": 0,
        "_priority_": 0,
        "_validator_": lambda m: True,
        "value": lambda m: str(m.group(1).strip()).lower()
    },
    {
        "grammar_symbol": "STAGE",
        "regex_type": "degree",
        "_regex_pattern_": r"(((I{0,1}V|I{1,3})\s*-?\s*){1,2})aste",
        "_group_": 0,
        "_priority_": 0,
        "_validator_": lambda m: True,
        "value": lambda m: clean_stage(m.group(1).strip())
    },
    {
        "grammar_symbol": "STAGE",
        "regex_type": "stage_3",
        "_regex_pattern_": r"NYHA[-= ]*(I[IV]*)",
        "_group_": 0,
        "_priority_": 0,
        "_validator_": lambda m: True,
        "value": lambda m: clean_stage(m.group(1).strip())
    },
    {
        "grammar_symbol": "STAGE",
        "regex_type": "stage_4",
        "_regex_pattern_": r"(I[IV]*)[- ]*NYHA",
        "_group_": 0,
        "_priority_": 0,
        "_validator_": lambda m: True,
        "value": lambda m: clean_stage(m.group(1).strip())
    },
    {
        "grammar_symbol": "STAGE",
        "regex_type": "stage_5",
        "_regex_pattern_": r"(^|[^0-9A-ZÜÕÖÄa-züõöä])(I[IV]*)($|[^0-9A-ZÜÕÖÄa-züõöä])",
        "_group_": 0,
        "_priority_": 1,
        "_validator_": lambda m: True,
        "value": lambda m: clean_stage(m.group(2).strip())
    },
    {
        "grammar_symbol": "DAMAGE",
        "regex_type": "damage_1",
        "_regex_pattern_": r"(organkahjustus|ok)[-= ]*(I[IV]*|\d)",
        "_group_": 0,
        "_priority_": 0,
        "_validator_": lambda m: True,
        "value": lambda m: clean_stage(m.group(2).strip())
    },
    {
        "grammar_symbol": "DAMAGE",
        "regex_type": "damage_2",
        "_regex_pattern_": r"(I[IV]*)[- ]*(organkahjustus|ok)",
        "_group_": 0,
        "_priority_": 0,
        "_validator_": lambda m: True,
        "value": lambda m: clean_stage(m.group(1).strip())
    },
    {
        "grammar_symbol": "SCORE",
        "regex_type": "stage_6",
        "_regex_pattern_": r"has[- ]*bled[-= ]*(\d)",
        "_group_": 0,
        "_priority_": 0,
        "_validator_": lambda m: True,
        "value": lambda m: m.group(1).strip()
    },
    {
        "grammar_symbol": "SCORE",
        "regex_type": "stage_7",
        "_regex_pattern_": r"CHA[-2DS ]*VASc[-= ]*(skoor)?[-= ]*(\d)",
        "_group_": 0,
        "_priority_": 0,
        "_validator_": lambda m: True,
        "value": lambda m: m.group(2).strip()
    },
    {
        "grammar_symbol": "SCORE",
        "regex_type": "stage_8",
        "_regex_pattern_": r"\( *([1-3]) *\)",
        "_group_": 0,
        "_priority_": 0,
        "_validator_": lambda m: True,
        "value": lambda m: m.group(1).strip()
    },
    {
        "grammar_symbol": "SCORE",
        "regex_type": "stage_9",
        "_regex_pattern_": r"ehra[-= ]*(skoor)?[-= ]*(\d)",
        "_group_": 0,
        "_priority_": 0,
        "_validator_": lambda m: True,
        "value": lambda m: m.group(2).strip()
    }
]
