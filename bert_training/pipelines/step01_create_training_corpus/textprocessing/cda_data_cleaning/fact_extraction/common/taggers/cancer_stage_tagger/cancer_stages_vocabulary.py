def clean_stage(input_stage):
    number_dict = {'1': 'I', '2': 'II', '3': 'III', '4': 'IV'}
    cleaned = "".join(input_stage.split(" ")).upper()
    for i in number_dict:
        cleaned = cleaned.replace(i, number_dict[i])
    return cleaned

def pick_tnm_codes(tnm_string):
    final_string = ""
    if tnm_string.find("T") >= 0 & tnm_string.find("N") >= 0 & tnm_string.find("M") >= 0:
        final_string += "T" + str(tnm_string[tnm_string.find("T")+1]) + ", "
        final_string += "N" + str(tnm_string[tnm_string.find("N")+1]) + ", "
        final_string += "M" + str(tnm_string[tnm_string.find("M")+1])
    return final_string

vocabulary = [
    {
        "grammar_symbol": "STAGE",
        "regex_type": "r1",
        "_regex_pattern_": r"(I[IV]* *[ABCabc]) *(st)?",
        "_group_": 0,
        "_priority_": 0,
        "_validator_": lambda m: True,
        "value": lambda m: clean_stage(m.group(1).strip()),
    },
    {
        "grammar_symbol": "STAGE",
        "regex_type": "r2",
        "_regex_pattern_": r"(I[IV]*) *st",
        "_group_": 0,
        "_priority_": 0,
        "_validator_": lambda m: True,
        "value": lambda m: clean_stage(m.group(1).strip()),
    },
    {
        "grammar_symbol": "STAGE",
        "regex_type": "r3",
        "_regex_pattern_": r"([0-5] *[ABCabc]) *st",
        "_group_": 0,
        "_priority_": 0,
        "_validator_": lambda m: True,
        "value": lambda m: clean_stage(m.group(1).strip()),
    },
    {
        "grammar_symbol": "STAGE",
        "regex_type": "r4",
        "_regex_pattern_": r"([0-5]) *st",
        "_group_": 0,
        "_priority_": 0,
        "_validator_": lambda m: True,
        "value": lambda m: clean_stage(m.group(1).strip()),
    },
    {
        "grammar_symbol": "STAGE",
        "regex_type": "r5",
        "_regex_pattern_": r"st\.? *([0-5] *[ABCabc]*)",
        "_group_": 0,
        "_priority_": 0,
        "_validator_": lambda m: True,
        "value": lambda m: clean_stage(m.group(1).strip()),
    },
    {
        "grammar_symbol": "STAGE",
        "regex_type": "r6",
        "_regex_pattern_": r"st\.? *(I[IV]* *[ABCabc]*)",
        "_group_": 0,
        "_priority_": 0,
        "_validator_": lambda m: True,
        "value": lambda m: clean_stage(m.group(1).strip()),
    },
    {
        "grammar_symbol": "TNM",
        "regex_type": "tnm",
        "_regex_pattern_": r"(T([0-4]|x)).*N.*M(\d|o)",
        "_priority_": 0,
        "_validator_": lambda m: True,
        "value": lambda m: pick_tnm_codes(m.group(0).strip())
    }
]
