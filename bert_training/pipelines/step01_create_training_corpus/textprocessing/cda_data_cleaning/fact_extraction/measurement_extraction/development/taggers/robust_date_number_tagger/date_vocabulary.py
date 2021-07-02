substitutions = {
    "LONGYEAR": r"(?P<LONGYEAR>((19[0-9]{2})|(20[0-9]{2})))",
    "YEAR": r"(?P<YEAR>((19[0-9]{2})|(20[0-9]{2})|([0-9]{2})))",
    "MONTH": r"(?P<MONTH>(0[1-9]|1[0-2]))",
    "DAY": r"(?P<DAY>(0[1-9]|[12][0-9]|3[01]))",
    "HOUR": r"(?P<hour>[0-2][0-9])",
    "MINUTE": r"(?P<minute>[0-5][0-9])",
    "SECOND": r"(?P<second>[0-5][0-9])",
}


vocabulary = [
    {
        "grammar_symbol": "DATE",
        "regex_type": "date2",
        "_regex_pattern_": r"{DAY}\.\s*{MONTH}\.\s*{YEAR}\s*{HOUR}[.:]{MINUTE}(:{SECOND})?".format(**substitutions),
        "_group_": 0,
        "_priority_": 2,
        "_validator_": lambda m: True,
        "value": "date_time",
    },
    {
        "grammar_symbol": "DATE",
        "regex_type": "date5",
        "_regex_pattern_": r"{YEAR}\.?\s*{MONTH}\.?\s*{DAY}".format(**substitutions),
        "_group_": 0,
        "_priority_": 4,
        "_validator_": lambda m: True,
        "value": "date",
    },
    {
        "grammar_symbol": "DATE",
        "regex_type": "date3",
        "_regex_pattern_": r"{DAY}\.\s*{MONTH}\.\s*{YEAR}[.a ]+\s*k(el)?l\.*\s*{HOUR}[.:]{MINUTE}(:{SECOND})?".format(
            **substitutions
        ),
        "_group_": 0,
        "_priority_": 2,
        "_validator_": lambda m: True,
        "value": "date_time",
    },
    {
        "grammar_symbol": "DATE",
        "regex_type": "date4",
        "_regex_pattern_": r"(?P<DAY>(0[1-9]|[12][0-9]|3[01]))\.\s*(?P<MONTH>(0[1-9]|1[0-2]))\.\s*{YEAR}".format(
            **substitutions
        ),
        "_group_": 0,
        "_priority_": -1,
        "_validator_": lambda m: True,
        "value": "date",
    },
]
