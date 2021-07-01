import re


def clean_stage(input_stage):
    number_dict = {'1': 'I', '2': 'II', '3': 'III', '4': 'IV'}
    cleaned = "".join(input_stage.split(" ")).upper()
    for i in number_dict:
        cleaned = cleaned.replace(i, number_dict[i])
    return cleaned


def pick_gleason(input_gleason):
    gleason_regex = r"\d\+\d(=\d)?"
    gleason_tag = re.search(gleason_regex, input_gleason)

    if gleason_tag is not None:
        return gleason_tag[0]
    return None


def pick_code(code):
    if code is None:
        return None

    T_value, N_value, M_value, G_value = None, None, None, None

    # lets remove any possible parenthesis inside our code (mostly for anonymous tags)
    anon_regex = r"\(.*\)"
    anon_tag = re.search(anon_regex, code)
    if anon_tag is not None:
        code = code.replace(anon_tag[0], "")

    # lets remove any gleason tags, which just add noise
    gleason_regex = r"Gleason \d\+\d(=\d)?"
    gleason_tag = re.search(gleason_regex, code)
    if gleason_tag is not None:
        code = code.replace(gleason_tag[0], "")

    for i in [".", ",", ";"]:
        code = code.replace(i, " ")

    # replacing character 'o'-s with number 0-s.
    for i in ["o", "O"]:
        code = code.replace(i, "0")

    # tegeleme G-ga enne, kuna too vahel on ja vahel mitte
    g_reg = r"G(\d|x)"
    g_reg_value = re.search(g_reg, code)
    if g_reg_value is not None:
        G_value = g_reg_value[0]
        code = code.replace(G_value, "")

    # Algab prefixiga, peale mida on vahe
    for i in ["p", "c"]:
        if code.startswith(i + " "):
            code = i + code[2:]

    pieces = code.split(" ")
    if len(pieces) == 3 or len(pieces) == 4:
        for i in pieces:
            if len(i) <= 6:
                if "T" in i.upper() and "N" not in i.upper() and "M" not in i.upper():
                    T_value = i
                elif "N" in i.upper() and "T" not in i.upper() and "M" not in i.upper():
                    N_value = i
                elif "M" in i.upper() and "T" not in i.upper() and "N" not in i.upper():
                    M_value = i
                elif "G" in i.upper():
                    G_value = i
        output_string = ", ".join([str(T_value), str(N_value), str(M_value)])

        if G_value is not None:
            output_string += ", " + str(G_value)

        if None not in (T_value, N_value, M_value):
            return output_string

    code = code.replace(" ", "")

    # peame eeldama, et kõik koodijupid on olemas järgnevaks regexiks
    for i in ["T", "M", "N"]:
        if i not in code:
            return None

    t_prefix_regex = r".*?(?=(?:T))"
    t_suffix_regex = r"^(\d[a-cA-C]?|x|is|)"

    n_prefix_regex = r".*?(?=(?:N))"
    n_suffix_regex = r"^(\d|x|)"

    m_prefix_regex = r".*?(?=(?:M))"
    # mis iganes üle jääb on M suffix

    # siin oli enne G

    t_pref = re.match(t_prefix_regex, code)[0]
    code = code[len(t_pref) + 1:]

    t_suffix = re.match(t_suffix_regex, code)[0]
    code = code[len(t_suffix):]

    n_pref = re.match(n_prefix_regex, code)[0]
    code = code[len(n_pref) + 1:]

    n_suffix = re.match(n_suffix_regex, code)[0]
    code = code[len(n_suffix):]

    m_pref = re.match(m_prefix_regex, code)[0]
    code = code[len(m_pref) + 1:]

    if len(code) <= 2:
        m_suffix = code
    elif code[0] in ['0', '1']:
        m_suffix = code[0]
    else:
        m_suffix = ""

    T_value = str(t_pref) + "T" + str(t_suffix)
    N_value = str(n_pref) + "N" + str(n_suffix)
    M_value = str(m_pref) + "M" + str(m_suffix)

    output_string = ", ".join([str(T_value), str(N_value), str(M_value)])
    if G_value is not None:
        output_string += ", " + str(G_value)



    return output_string


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
        "grammar_symbol": "Gleason",
        "regex_type": "gleason",
        "_regex_pattern_": r"Gleason \d\+\d(=\d)?",
        "_priority_": 0,
        "_group_": 0,
        "_validator_": lambda m: True,
        "value": lambda m: pick_gleason(m.group(0))
    },
    {
        "grammar_symbol": "TNM",
        "regex_type": "tnm",
        "_regex_pattern_": r"(r|y)?((p|c) )?(G.*)?.?(T((\d|x).?)).{0,1}N.{0,4}M.{0,1}((.*G)?[^\s]?(\d|x)+)?",
        "_priority_": 1,
        "_group_": 0,
        "_validator_": lambda m: True,
        "value": lambda m: pick_code(m.group(0))
    }
]
