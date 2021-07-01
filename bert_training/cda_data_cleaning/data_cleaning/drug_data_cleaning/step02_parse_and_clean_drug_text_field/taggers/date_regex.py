import re

MONTHS = {
    "jaanuar": 1,
    "veebruar": 2,
    "märts": 3,
    "aprill": 4,
    "mai": 5,
    "juuni": 6,
    "juuli": 7,
    "august": 8,
    "seprember": 9,
    "oktoober": 10,
    "november": 11,
    "detsember": 12,
}

MACROS = {
    "hour": "[0-2][0-9]",
    "minute": "[0-5][0-9]",
    "second": "[0-5][0-9]",
    "DAY": "[0-3]?[0-9]",
    "MONTH": "(0?[1-9]|1[0-2])",
    "MONTHSTR": "(%s)" % "|".join(MONTHS.keys()),
    "YEAR": "((19[0-9]{2})|(20[0-9]{2})|([0-9]{2}))",
    "LONGYEAR": "((19[0-9]{2})|(20[0-9]{2}))",
}

for k, v in MACROS.items():
    MACROS[k] = r"(?P<{key}>{regex})".format(key=k, regex=v)

MACROS["DATE"] = r"{DAY}((\.?\s*)|(\s*)){MONTH}((\.?\s*)|(\s*)){LONGYEAR}".format(**MACROS)
MACROS["DATE2"] = r"{LONGYEAR}((\.?\s*)|(\s*)){MONTH}((\.?\s*)|(\s*)){DAY}".format(**MACROS)
MACROS["DATESTR"] = r"{DAY}((\.\s*)|(\s*)){MONTHSTR}((\.\s*)|(\s*)){YEAR}".format(**MACROS)
MACROS["TIME"] = r"{hour}[:.]{minute}(:{second})?".format(**MACROS)

REGEX = {
    # {'regex': 'k(el)?l\s{TIME}'.format(**MACROS), 'type': 'time', 'probability': '1.0', 'example': 'kell 21:30'},
    # {'regex': '(^|[^0-9]){DATE}\s*{TIME}'.format(**MACROS), 'type': 'date_time', 'probability': '0.9', 'example': '21.03.2015 15:30:45'},
    # {'regex': '(^|[^0-9]){DATESTR}\s*{TIME}'.format(**MACROS), 'type': 'date_time', 'probability': '0.9', 'example': '21 märts 2015 15:30:45'},
    # {'regex': '(^|[^0-9]){DATE}[.a ]+\s*k(el)?l\.*\s*{TIME}'.format(**MACROS), 'type': 'date_time', 'probability': '1.0', 'example':'21.03.2015. kell 15:30'},
    # {'regex': '(^|[^0-9]){DATESTR}[.a ]+\s*k(el)?l\.*\s*{TIME}'.format(**MACROS), 'type': 'date_time', 'probability': '1.0', 'example':'21 märts 2015. kell 15:30'},
    "regex": "(^|[^0-9]){DATE}($|[^0-9])".format(**MACROS),
    "regex2": "(^|[^0-9]){DATE2}($|[^0-9])".format(**MACROS)
    # {'regex': '(^|[^0-9]){DATESTR}($|[^0-9])'.format(**MACROS), 'type': 'date', 'probability': '0.8', 'example':'12 jaanuar 98'},
    # {'regex': '(^|\s){DAY}\.\s?{MONTH}($|[^0-9])($|\s)'.format(**MACROS), 'type': 'partial_date', 'probability': '0.3', 'example': '03.12'},
    # {'regex': '(^|[^0-9]){MONTH}\.\s?{LONGYEAR}($|[^0-9])'.format(**MACROS), 'type': 'partial_date', 'probability': '0.6', 'example': '03.2012'},
    # {'regex': '(^|[^0-9]){DAY}\.\s?{MONTH}\s*k(el)?l\s{TIME}'.format(**MACROS), 'type': 'partial_date', 'probability': '0.8', 'example': '03.01 kell 10.20'},
    # {'regex': '(^|[^0-9]){LONGYEAR}\s*a'.format(**MACROS), 'type': 'partial_date', 'probability': '0.8', 'example': '1998a'},
    # {'regex': '(^|[^0-9]){LONGYEAR}\s*\.a'.format(**MACROS), 'type': 'partial_date', 'probability': '0.8', 'example': '1998.a'},
    # {'regex': '(^|[^0-9]){LONGYEAR}($|[^0-9])'.format(**MACROS), 'type': 'partial_date', 'probability': '0.4', 'example': '1998'}
}

regexes = {}
for reg_name, reg in REGEX.items():
    # for pre_name, pre in PREFIXES.items():
    regexes[reg_name] = re.compile(reg)
