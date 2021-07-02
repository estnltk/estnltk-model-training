import marisa_trie
from configuration import database_connection_string
from legacy.utils import replace_chars
import sqlalchemy as sq
import Levenshtein


def query_icd10_data(connection):
    query_sql = """select icd10_code, icd10_name_et_split, icd10_name_latin_split from classifications.diagnoses;"""

    sql = sq.text(query_sql)

    print("Starting query.")
    result = connection.execute(sql)
    print("Query done.")

    return result


def create_alternative_names(name):
    replaceables = ["õ", "ä", "ö", "ü"]
    shorts = {
        "pahaloomuline kasvaja": "pk",
        "täpsustamata": "tta",
        "täpsustatud": "ttud",
        "kokkupuude": "kokkup",
        "mujal klassifitseerimata": "mkta",
        "healoomuline kasvaja": "hk",
    }
    names = []

    # replace all with � or _
    if len(set(list(name)).intersection(set(replaceables))) > 0:
        names.append(replace_chars(name, replaceables, "�"))
        names.append(replace_chars(name, replaceables, "_"))
    if "õ" in name:
        names.append(replace_chars(name, ["õ"], "ō"))

    shortened_names = []
    for short in shorts:
        if short in name:
            for alternative_name in names + [name]:
                shortened_names.append(alternative_name.replace(short, shorts[short]))

    return names + shortened_names


def add_one_icd(keys, values, code, raw_name):
    names = [x.strip() for x in raw_name.split("¦")] + [raw_name]
    for name in names:
        alternative_names = create_alternative_names(name)
        for final_name in alternative_names + [name]:
            keys.append(final_name)
            values.append((str.encode(code.strip()),))


def generate_trie(result):
    keys_et = []
    keys_ru = []
    values_et = []
    values_ru = []
    for res in result:
        code = res["icd10_code"].upper().strip()
        raw_name_et = res["icd10_name_et_split"].lower().strip()
        raw_name_ru = res["icd10_name_latin_split"].lower().strip()
        add_one_icd(keys_et, values_et, code, raw_name_et)
        add_one_icd(keys_ru, values_ru, code, raw_name_ru)
    fmt = "255p"
    trie_et = marisa_trie.RecordTrie(fmt, zip(keys_et, values_et))
    trie_ru = marisa_trie.RecordTrie(fmt, zip(keys_ru, values_ru))
    return {"et": trie_et, "ru": trie_ru}


def generate_code_dict(result):
    code_dict = {}
    for res in result:
        code = res["icd10_code"].upper().strip()
        raw_name_et = res["icd10_name_et_split"].lower().strip()
        raw_name_ru = res["icd10_name_latin_split"].lower().strip()
        names_et = [x.strip() for x in raw_name_et.split("¦")] + [raw_name_et]
        names_ru = [x.strip() for x in raw_name_ru.split("¦")] + [raw_name_ru]
        if code not in code_dict:
            code_dict[code] = {"et": [], "ru": []}
        for name in names_et:
            code_dict[code]["et"].append(name)
        for name in names_ru:
            code_dict[code]["ru"].append(name)
    return code_dict


class ICD10:
    def __init__(self, name_tries, code_dict):
        self.name_tries = name_tries
        self.code_dict = code_dict

    def query_parent_names(self, icd10_code, icd10_name, lang):
        parent_names = []
        parent_code = icd10_code
        final_names = []
        while len(parent_code) > 0 and parent_code in self.code_dict:
            parent_name = self.code_dict[parent_code][lang]
            if parent_names == []:
                for name in parent_name:
                    parent_names.append(name)
            else:
                new_names = []
                for new_name in parent_name:
                    for old_name in parent_names:
                        new_names.append(new_name + ", " + old_name)
                parent_names = new_names
                final_names += new_names
            # parent_names = [parent_name[0]] + parent_names
            parent_code = parent_code[:-1].strip(".")

        for name in final_names:
            ratio = Levenshtein.ratio(name, icd10_name)
            if ratio == 1:
                return ["parents joined match", icd10_code, icd10_name, True]
            elif ratio >= 0.75:
                return ["parents joined similar match", icd10_code, icd10_name, True]

        return None

    def query_by_code(self, icd10_name, icd10_code):
        names = self.code_dict[icd10_code] if icd10_code in self.code_dict else {"et": [], "ru": []}
        for lang in names:
            for name in names[lang]:
                if name == icd10_name:
                    return ["exact match", icd10_code, name, True]
                ratio = Levenshtein.ratio(icd10_name, name)
                if ratio >= 0.75:
                    # return [lang + ';similar match', icd10_code, name]
                    return ["similar match", icd10_code, name, True]

        for lang in names:
            for name in names[lang]:
                # s = difflib.SequenceMatcher(None, icd10_name, name)
                # ratio = s.ratio()
                if name in icd10_name:
                    parent_query_res = self.query_parent_names(icd10_code, icd10_name, lang)
                    if parent_query_res:
                        return parent_query_res
                    # return [lang + ';rhk sub of name', icd10_code, name]
                    return ["ICD10 sub of name", None, None, False]
                elif icd10_name in name:
                    # return [lang + ';name sub of rhk', icd10_code, name]
                    return ["name sub of ICD10", None, None, False]

        for lang in names:
            for name in names[lang]:
                parts = icd10_name.split()
                _in = True
                for part in parts:
                    if part not in name:
                        _in = False
                        break
                if _in:
                    return ["parts in ICD10", None, None, False]

        return ["no match", None, None, False]

    def query(self, icd10_name, icd10_code):
        if icd10_code not in self.code_dict:
            return ["unknown code", None, None, False]

        et_query = self.query_trie(self.name_tries["et"], icd10_name, icd10_code)
        if et_query[0] != "no match":
            return et_query

        ru_query = self.query_trie(self.name_tries["ru"], icd10_name, icd10_code)  # , 'ru;')
        if ru_query[0] != "no match":
            return ru_query

        return self.query_by_code(icd10_name, icd10_code)

    def query_trie(self, trie, icd10_name, icd10_code, language=""):
        icd10_name = icd10_name.replace("ō", "õ")
        matches = trie.get(icd10_name, [])
        matched_codes = list(set([matches[i][0].decode("ascii").strip() for i in range(len(matches))]))

        if len(matched_codes) == 1 and matched_codes[0] == icd10_code:
            return [language + "exact match", matched_codes[0], icd10_name, True]
        elif len(matched_codes) == 1 and matched_codes[0] != icd10_code:
            if matched_codes[0].startswith(icd10_code):
                return [language + "exact match; child code", matched_codes[0], icd10_name, True]
            elif icd10_code.startswith(matched_codes[0]):
                return [language + "exact match; parent code", matched_codes[0], icd10_name, True]
            return [language + "exact match; code mismatch", None, None, False]
        if len(matched_codes) > 1 and icd10_code in matched_codes:
            return [language + "> 1 match; code match", None, None, False]
        elif (len(matched_codes)) > 1 and icd10_code not in matched_codes:
            return [language + "> 1 match; code mismatch", None, None, False]

        prefix_matches = trie.items(icd10_name)
        prefix_matched_codes = (
            list(set([prefix_matches[i][1][0].decode("ascii").strip() for i in range(len(prefix_matches))]))
            if prefix_matches
            else []
        )

        prefix_matched_names = (
            list(set([prefix_matches[i][0].strip() for i in range(len(prefix_matches))])) if prefix_matches else []
        )

        if len(prefix_matched_codes) == 1 and prefix_matched_codes[0] == icd10_code:
            return [language + "prefix match", prefix_matched_codes[0], prefix_matched_names[0], True]
        elif len(prefix_matched_codes) == 1 and prefix_matched_codes[0] != icd10_code:
            if prefix_matched_codes[0].startswith(icd10_code):
                return [language + "prefix match; child code", prefix_matched_codes[0], prefix_matched_names[0], True]
            elif icd10_code.startswith(prefix_matched_codes[0]):
                return [language + "prefix match; parent code", prefix_matched_codes[0], prefix_matched_names[0], True]
            return [language + "prefix match; code mismatch", None, None, False]
        if len(prefix_matched_codes) > 1 and icd10_code in prefix_matched_codes:
            return [language + "> 1 prefix match; code match", None, None, False]
        elif (len(prefix_matched_codes)) > 1 and icd10_code not in prefix_matched_codes:
            return [language + "> 1 prefix match; code mismatch", None, None, False]

        return ["no match", None, None, False]


def get_icd10_data():
    engine = sq.create_engine(database_connection_string, encoding="utf-8", convert_unicode=True)

    connection = engine.connect()
    meta = sq.MetaData(bind=connection)
    meta.reflect(connection)

    with connection.begin():
        result = query_icd10_data(connection)
        trie = generate_trie(result)
        result = query_icd10_data(connection)
        code_dict = generate_code_dict(result)

    # print(len(trie.keys()))
    # print(trie.items('Konkussioon'))
    # print(trie.get('Konkussioon', None))

    return trie, code_dict


def test():
    tries, cd = get_icd10_data()
    icd10 = ICD10(tries, cd)
    res = icd10.query("onychomycosis", "B35.1")
    print(res)
    # print(tries['et'].items('südamekahjustusega hüpertooniatõbi ilma (kongestiivse) südamepuudulikkuseta'))


# test()
