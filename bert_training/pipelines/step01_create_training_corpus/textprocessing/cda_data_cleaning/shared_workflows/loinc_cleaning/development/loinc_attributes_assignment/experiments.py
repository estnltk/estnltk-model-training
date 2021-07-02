import itertools
import json
import os
import pathlib

import numpy as np
import pandas as pd
import sqlalchemy as sq
from sqlalchemy.exc import ProgrammingError

from configuration import WORK_SCHEMA, database_connection_string

UNK = "unknown"


# see fail on kummaline sest on tehtud ipython notebooki põhjal.
# vt https://git.stacc.ee/project4-egcut/LOINCtagger/blob/master/experiments.ipynb


def apply_rules(dataframe):
    dataframe.loc[(dataframe.analyte == "PLT") & dataframe.unit.str.contains("U"), "property"] = UNK

    dataframe.loc[
        (dataframe.analyte == "PLT") & ~dataframe.unit.str.contains("U") & dataframe.unit.str.contains("g"), "property"
    ] = UNK
    dataframe.loc[
        (dataframe.analyte == "PLT")
        & ~dataframe.unit.str.contains("U")
        & ((dataframe.unit == "") | (dataframe.unit == ".")),
        "property",
    ] = UNK

    # 10 L on andmete põhjal loogiline kui eeldada, et aste on kaduma läinud
    # peaks olema (10E9/L)

    dataframe.loc[
        (dataframe.analyte == "PLT") & ~dataframe.unit.str.contains("U") & (dataframe.property != UNK), "property"
    ] = "NCnc"

    dataframe.loc[(dataframe.analyte == "HCT"), "property"] = "VFr"

    dataframe.loc[(dataframe.analyte == "WBC") & dataframe.unit.str.contains("U"), "property"] = UNK

    dataframe.loc[(dataframe.analyte == "WBC") & dataframe.unit.str.contains("g"), "property"] = UNK

    dataframe.loc[(dataframe.analyte == "WBC") & (dataframe.unit == "/100 WBC"), "property"] = UNK

    # NB! veaparandus analüüdis
    dataframe.loc[(dataframe.analyte == "WBC") & (dataframe.unit == "/100 WBC"), "analyte"] = UNK

    dataframe.loc[(dataframe.analyte == "WBC") & (dataframe.property == ""), "property"] = "NCnc"

    # vahelduseks hea ja lihtne reegel
    dataframe.loc[(dataframe.analyte == "MCHC"), "property"] = "MCnc"

    dataframe.loc[(dataframe.analyte == "RBC") & dataframe.unit.str.contains("U"), "property"] = UNK

    dataframe.loc[(dataframe.analyte == "RBC") & (dataframe.property == ""), "property"] = "NCnc"

    dataframe.loc[(dataframe.analyte == "MCV"), "property"] = "EntVol"

    dataframe.loc[(dataframe.analyte == "MCH") & (dataframe.unit == "g/L"), "property"] = "MCnc"

    # klassifitseerime paremaks
    dataframe.loc[(dataframe.analyte == "MCH") & (dataframe.property == "MCnc"), "analyte"] = "MCHC"

    dataframe.loc[(dataframe.analyte == "MCH"), "property"] = "EntMass"

    dataframe.loc[(dataframe.analyte == "HGB"), "property"] = "MCnc"

    # active[(active.analyte == 'MPV') & (active.unit == 'fL')] #EntVol
    dataframe.loc[(dataframe.analyte == "MPV") & (dataframe.unit == "fL"), "property"] = "EntVol"
    dataframe.loc[(dataframe.analyte == "MPV") & (dataframe.unit == "%"), "property"] = UNK

    # andmetele peale vaadates tundus õige
    dataframe.loc[(dataframe.analyte == "MPV") & (dataframe.property == ""), "property"] = "EntVol"

    dataframe.loc[(dataframe.analyte == "PDW") & (dataframe.unit == "fL"), "property"] = "EntVol"
    dataframe.loc[(dataframe.analyte == "PDW") & (dataframe.unit == "%"), "property"] = "Ratio"
    dataframe.loc[(dataframe.analyte == "PDW") & (dataframe.property == ""), "property"] = UNK

    dataframe.loc[(dataframe.analyte == "Erikaal SG"), "property"] = "Rden"

    dataframe.loc[(dataframe.analyte == "HbA1c") & (dataframe.unit == "%"), "property"] = "MFr"
    dataframe.loc[(dataframe.analyte == "HbA1c") & (dataframe.unit == "mmol/mol"), "property"] = "SFr"

    # see ei tundu loogiline
    dataframe.loc[(dataframe.analyte == "HbA1c") & (dataframe.unit == "mmol/L"), "property"] = UNK

    dataframe.loc[(dataframe.analyte == "HbA1c") & (dataframe.property == ""), "property"] = "MFr"

    dataframe.loc[(dataframe.analyte == "INR") & (dataframe.unit == "%"), "property"] = "RelTime"

    # Tundub küsitav
    dataframe.loc[(dataframe.analyte == "INR") & (dataframe.unit == "sek"), "property"] = UNK

    dataframe.loc[(dataframe.analyte == "INR") & (dataframe.property == ""), "property"] = "RelTime"

    # Tundub, et on vigased adnmeread
    dataframe.loc[(dataframe.analyte == "Crea") & (dataframe.unit == "mL/min/1.73 m2"), "property"] = UNK
    dataframe.loc[(dataframe.analyte == "Crea") & (dataframe.unit == "mL/min"), "property"] = UNK
    dataframe.loc[(dataframe.analyte == "Crea") & (dataframe.property == ""), "property"] = "SCnc"

    dataframe.loc[(dataframe.analyte == "ALAT") & (dataframe.unit == "U/L"), "property"] = "CCnc"

    dataframe.loc[(dataframe.analyte == "ALAT") & (dataframe.property == ""), "property"] = "CCnc"

    # See tundub jama
    dataframe.loc[(dataframe.analyte == "ALAT") & (dataframe.unit == "mmol/L"), "property"] = UNK

    dataframe.loc[(dataframe.analyte == "ASAT") & (dataframe.unit == "U/L"), "property"] = "CCnc"

    dataframe.loc[(dataframe.analyte == "ASAT") & (dataframe.property == ""), "property"] = "CCnc"

    # See tundub jama
    dataframe.loc[(dataframe.analyte == "ASAT") & (dataframe.unit == "mmol/L"), "property"] = UNK

    dataframe.loc[
        (dataframe.analyte == "TSH") & (dataframe.property == "") & (dataframe.unit.str.contains("U")), "property"
    ] = "ACnc"
    dataframe.loc[
        (dataframe.analyte == "TSH") & (dataframe.property == "") & (dataframe.unit == "uL/mL"), "property"
    ] = UNK
    dataframe.loc[(dataframe.analyte == "TSH") & (dataframe.property == ""), "property"] = "ACnc"

    dataframe.loc[
        (dataframe.analyte == "Prot") & (dataframe.property == "") & (dataframe.unit.str.contains("g/L")), "property"
    ] = "MCnc"

    dataframe.loc[
        (dataframe.analyte == "Prot") & (dataframe.property == "") & (dataframe.system_type == "blood"), "property"
    ] = "MCnc"

    dataframe.loc[
        (dataframe.analyte == "Prot") & (dataframe.property == "") & (dataframe.unit.str.contains("g/24h")), "property"
    ] = "MRat"

    # Need peaksid olema uriinianalüüsid, aga vajavad veel paremini lahku löömist
    dataframe.loc[(dataframe.analyte == "Prot") & (dataframe.property == ""), "property"] = UNK

    dataframe.loc[(dataframe.analyte == "CRP") & (dataframe.property == ""), "property"] = "MCnc"

    dataframe.loc[(dataframe.analyte == "Urea") & (dataframe.property == ""), "property"] = "SCnc"

    dataframe.loc[(dataframe.analyte == "LCR") & (dataframe.property == ""), "property"] = "NFr"

    dataframe.loc[(dataframe.analyte == "Alfa-amülaas"), "analyte"] = "Amyl"

    dataframe.loc[(dataframe.analyte == "Amyl") & (dataframe.property == ""), "property"] = "CCnc"

    dataframe.loc[(dataframe.analyte == "LDL") & (dataframe.property == ""), "property"] = "SCnc"

    dataframe.loc[
        (dataframe.analyte == "Leukotsüüdid") & (dataframe.property == "") & (dataframe.unit == "r/vv"), "property"
    ] = "Naric"  #

    dataframe.loc[
        (dataframe.analyte == "Leukotsüüdid")
        & (dataframe.property == "")
        & (dataframe.parameter_name.str.contains("mikroskoop")),
        "property",
    ] = "Naric"  #
    dataframe.loc[
        (dataframe.analyte == "Leukotsüüdid")
        & (dataframe.property == "")
        & (dataframe.analysis_name.str.contains("mikroskoop")),
        "property",
    ] = "Naric"  #

    dataframe.loc[(dataframe.analyte == "Leukotsüüdid") & (dataframe.property == ""), "property"] = "NCnc"

    dataframe.loc[(dataframe.analyte == "HDL") & (dataframe.property == ""), "property"] = "SCnc"

    dataframe.loc[(dataframe.analyte == "Trigl") & (dataframe.property == ""), "property"] = "SCnc"

    dataframe.loc[
        (dataframe.analyte == "Erütrotsüüdid") & (dataframe.property == "") & (dataframe.unit == "r/vv"), "property"
    ] = "Naric"  #

    dataframe.loc[
        (dataframe.analyte == "Erütrotsüüdid")
        & (dataframe.property == "")
        & (dataframe.parameter_name.str.contains("mikroskoop")),
        "property",
    ] = "Naric"  #
    dataframe.loc[
        (dataframe.analyte == "Erütrotsüüdid")
        & (dataframe.property == "")
        & (dataframe.analysis_name.str.contains("mikroskoop")),
        "property",
    ] = "Naric"  #
    dataframe.loc[(dataframe.analyte == "Erütrotsüüdid") & (dataframe.property == ""), "property"] = "NCnc"

    dataframe.loc[(dataframe.analyte == "Na") & (dataframe.property == ""), "property"] = "SCnc"

    dataframe.loc[(dataframe.analyte == "GGT") & (dataframe.property == ""), "property"] = "CCnc"

    dataframe.loc[(dataframe.analyte == "bilirubiin") & (dataframe.property == ""), "analyte"] = "Bil"
    dataframe.loc[(dataframe.analyte == "Bil") & (dataframe.property == ""), "property"] = "SCnc"

    # Siin jagunevad neljaks
    # Mõõdetakse (kreatiini kinaasi fraktsiooni e. isoensüümi) ja (kreatiini kinaasi)
    dataframe.loc[
        (dataframe.analyte == "CK")
        & (dataframe.property == "")
        & (dataframe.parameter_name.str.contains("MB") | dataframe.analysis_name.str.contains("MB")),
        "analyte",
    ] = "CK-MB"

    dataframe.loc[
        ((dataframe.analyte == "CK") | (dataframe.analyte == "CK-MB"))
        & (dataframe.property == "")
        & dataframe.unit.str.contains("U"),
        "property",
    ] = "CCnc"

    dataframe.loc[
        ((dataframe.analyte == "CK") | (dataframe.analyte == "CK-MB"))
        & (dataframe.property == "")
        & (dataframe.unit.str.contains("g") | dataframe.parameter_name.str.contains("MBm")),
        "property",
    ] = "MCnc"

    dataframe.loc[
        ((dataframe.analyte == "CK") | (dataframe.analyte == "CK-MB")) & (dataframe.property == ""), "property"
    ] = "CCnc"

    dataframe.loc[(dataframe.analyte == "UA") & (dataframe.property == ""), "property"] = "SCnc"

    dataframe.loc[(dataframe.analyte == "K") & (dataframe.property == ""), "property"] = "SCnc"

    dataframe.loc[(dataframe.analyte == "Chol") & (dataframe.property == ""), "property"] = "SCnc"

    dataframe.loc[
        (dataframe.analyte == "Alb")
        & (dataframe.property == "")
        & (dataframe.parameter_name == "S-Alb/Gl Albumiinid / Globuliinid"),
        "analyte",
    ] = UNK

    dataframe.loc[(dataframe.analyte == "Alb") & (dataframe.property == ""), "property"] = "MCnc"

    dataframe.loc[(dataframe.analyte == "pH") & (dataframe.property == ""), "property"] = "LsCnc"

    dataframe.loc[
        (dataframe.analyte == "Fe") & (dataframe.property == "") & (dataframe.unit.str.contains("g")), "property"
    ] = "MCnc"
    dataframe.loc[(dataframe.analyte == "Fe") & (dataframe.property == ""), "property"] = "SCnc"

    dataframe.loc[(dataframe.analyte == "ALP") & (dataframe.property == ""), "property"] = "CCnc"

    dataframe.loc[
        (dataframe.analyte == "IG")
        & (dataframe.property == "")
        & ((dataframe.unit == "%") | dataframe.parameter_name.str.contains("%")),
        "property",
    ] = "NFr"

    dataframe.loc[
        (dataframe.analyte == "IG")
        & (dataframe.property == "")
        & (dataframe.unit.str.contains("L") | (dataframe.unit.str.contains("#"))),
        "property",
    ] = "NCnc"

    # Ei ole selge
    dataframe.loc[(dataframe.analyte == "IG") & (dataframe.property == ""), "property"] = UNK

    dataframe.loc[
        (dataframe.analyte == "RF") & (dataframe.property == "") & ((dataframe.unit.str.contains("U"))), "property"
    ] = "ACnc"

    dataframe.loc[
        (dataframe.analyte == "RF") & (dataframe.property == "") & ((dataframe.unit == "")), "property"
    ] = "ACnc"

    dataframe.loc[(dataframe.analyte == "RF") & (dataframe.property == ""), "property"] = UNK

    dataframe.loc[
        (dataframe.analyte == "LDH") & (dataframe.property == "") & ((dataframe.unit.str.contains("U"))), "property"
    ] = "CCnc"

    dataframe.loc[(dataframe.analyte == "Bil_conj") & (dataframe.property == ""), "property"] = "SCnc"

    dataframe.loc[(dataframe.analyte == "Ca") & (dataframe.property == ""), "property"] = "SCnc"

    dataframe.loc[(dataframe.analyte == "iCa") & (dataframe.property == ""), "property"] = "SCnc"

    dataframe.loc[(dataframe.analyte == "Mg") & (dataframe.property == ""), "property"] = "SCnc"

    dataframe.loc[(dataframe.analyte == "ASO") & (dataframe.property == ""), "property"] = "ACnc"

    dataframe.loc[
        (dataframe.analyte == "RDW") & (dataframe.property == "") & (dataframe.parameter_name.str.contains("CV")),
        "property",
    ] = "Ratio"

    dataframe.loc[
        (dataframe.analyte == "RDW") & (dataframe.property == "") & (dataframe.unit.str.contains("CV")), "property"
    ] = "Ratio"

    dataframe.loc[
        (dataframe.analyte == "RDW") & (dataframe.property == "") & (dataframe.unit.str.contains("L")), "property"
    ] = "EntVol"

    dataframe.loc[
        (dataframe.analyte == "RDW") & (dataframe.property == "") & (dataframe.parameter_name.str.contains("SD")),
        "property",
    ] = "EntVol"

    dataframe.loc[(dataframe.analyte == "RDW") & (dataframe.property == ""), "property"] = UNK

    dataframe.loc[
        (dataframe.analyte == "ESR") & (dataframe.property == "") & (dataframe.unit == "mm/h"), "property"
    ] = "Vel"

    dataframe.loc[(dataframe.analyte == "ESR") & (dataframe.property == ""), "property"] = UNK

    dataframe.loc[(dataframe.analyte == "Urobilinogeen UBG") & (dataframe.property == ""), "property"] = "SCnc"

    # Ühikuid millegipärast pole.
    # Eestis mõõdetakse mmol/L
    dataframe.loc[(dataframe.analyte == "Gluc") & (dataframe.property == ""), "property"] = "SCnc"

    dataframe.loc[(dataframe.analyte == "Trombokrit") & (dataframe.property == ""), "property"] = "VFr"

    dataframe.loc[(dataframe.analyte == "Mono") & (dataframe.property == ""), "property"] = UNK
    dataframe.loc[(dataframe.analyte == "Lymph") & (dataframe.property == ""), "property"] = UNK
    dataframe.loc[(dataframe.analyte == "Eo") & (dataframe.property == ""), "property"] = UNK
    dataframe.loc[(dataframe.analyte == "Baso") & (dataframe.property == ""), "property"] = UNK
    dataframe.loc[(dataframe.analyte == "Neut") & (dataframe.property == ""), "property"] = UNK

    # Need tuleks jagada suhteliseks ja absoluutseks
    # aga LOINC pakub suhtelisele Ratio ja NFr tõlgendusi.
    # Kuna hetkel on neid andmestikus vähe, jätan klassifitseerimata
    dataframe.loc[(dataframe.analyte == "NRBC") & (dataframe.property == ""), "property"] = UNK

    dataframe.loc[
        (dataframe.analyte == "Transf") & (dataframe.property == "") & (dataframe.unit.str.contains("g")), "property"
    ] = "MCnc"

    # http://www.kliinikum.ee/yhendlabor/labor/index.php?mod=indeks_view&id=321
    dataframe.loc[
        (dataframe.analyte == "Transf")
        & (dataframe.property == "")
        & (dataframe.parameter_name.str.contains("saturatsioon")),
        "analyte",
    ] = "sTransf"

    dataframe.loc[(dataframe.analyte == "Transf") & (dataframe.property == ""), "property"] = "MCnc"

    dataframe.loc[(dataframe.analyte == "Kreatiniin") & (dataframe.unit == "mmol/L"), "property"] = "SCnc"
    dataframe.loc[(dataframe.analyte == "Kreatiniin") & (dataframe.unit == "mg/dL"), "property"] = "MCnc"
    dataframe.loc[(dataframe.analyte == "Kreatiniin") & (dataframe.unit == ""), "property"] = UNK
    dataframe.loc[(dataframe.analyte == "Kreatiniin"), "analyte"] = "Creat"

    dataframe.loc[(dataframe.analyte == "P"), "property"] = "SCnc"

    # Ühikuid pole
    dataframe.loc[(dataframe.analyte == "LDH") & (dataframe.property == ""), "property"] = UNK

    # active[(active.analyte == 'IgE') & (active.property == '')]
    dataframe.loc[(dataframe.analyte == "IgE") & (dataframe.property == ""), "property"] = "ACnc"

    dataframe.loc[(dataframe.analyte == "ketoonid") & (dataframe.property == ""), "property"] = "SCnc"

    # active[(active.analyte == 'PCT') & (active.property == '')]
    dataframe.loc[(dataframe.analyte == "PCT") & (dataframe.property == ""), "property"] = "MCnc"

    # Epithelial cells.squamous
    dataframe.loc[(dataframe.analyte == "Lameepiteeli rakud") & (dataframe.property == ""), "property"] = "Naric"

    dataframe.loc[(dataframe.analyte == "Transitoorsed epiteelid") & (dataframe.property == ""), "property"] = "Naric"
    dataframe.loc[(dataframe.analyte == "Tuubulusepiteelid") & (dataframe.property == ""), "property"] = "Naric"

    dataframe.loc[(dataframe.analyte == "IgG") & (dataframe.property == ""), "property"] = "MCnc"
    dataframe.loc[(dataframe.analyte == "IgA") & (dataframe.property == ""), "property"] = "MCnc"
    dataframe.loc[(dataframe.analyte == "IgM") & (dataframe.property == ""), "property"] = "MCnc"

    dataframe.loc[
        (dataframe.analyte == "glükoos") & (dataframe.property == "") & (dataframe.unit.str.contains("mmol")),
        "property",
    ] = "SCnc"
    dataframe.loc[(dataframe.analyte == "glükoos") & (dataframe.property == ""), "property"] = UNK

    dataframe.loc[(dataframe.analyte == "glükoos"), "analyte"] = "Gluc"

    dataframe.loc[(dataframe.analyte == "Cl"), "property"] = "SCnc"

    dataframe.loc[(dataframe.analyte == "Düsmorfsed erütrotsüüdid") & (dataframe.property == ""), "property"] = "Naric"

    # absoluut või protsent, hetkel liiga vähe infot
    dataframe.loc[(dataframe.analyte == "Ret"), "property"] = UNK

    dataframe.loc[
        (dataframe.analyte == "Amyl")
        & (
            dataframe.parameter_name.str.contains("[pP]ankreas")
            | (dataframe.parameter_name.str.contains("AmylP"))
            | (dataframe.parameter_name.str.contains("pancr"))
        ),
        "analyte",
    ] = "AmylP"

    # Ühikuid pole
    # Kõik väärtused tabelis nullid.
    # tulemus võiks tegelikult olla neg-pos?
    dataframe.loc[(dataframe.analyte == "nitritid"), "property"] = UNK

    # # Splitting blood classes

    dataframe.loc[dataframe.property == "", "property"] = None
    dataframe.loc[dataframe.analyte == "", "analyte"] = None
    dataframe.loc[dataframe.parameter_code == "", "parameter_code"] = None
    dataframe.loc[dataframe.parameter_name == "", "parameter_name"] = None
    dataframe.loc[dataframe.analysis_name == "", "analysis_name"] = None
    dataframe.loc[dataframe.unit == "", "unit"] = None
    return dataframe


def create_merged_table(columns, df, urine_analytes, blood_analytes):

    res = {"bl": blood_analytes[columns], "ur": urine_analytes[columns]}
    # võtab välja vere analüüsid
    blood_analytes = blood_analytes[np.array([i in res["bl"].index for i in blood_analytes.index])]
    urine_analytes = urine_analytes[np.array([i in res["ur"].index for i in urine_analytes.index])]

    # paneb üksteise järele
    r = pd.concat([blood_analytes, urine_analytes])

    # tablei ühendamine isenendaga ehk join, columnsid peaavd kokku langema
    merge = pd.merge(df, r, on=columns, how="outer", indicator=False)
    # süsteemi tüüpi uuendatakse
    merge["system_type"] = merge["system_type_x"]
    merge = merge[["analysis_name", "parameter_code", "parameter_name", "unit", "count", "system_type", "analyte"]]
    merge = merge[merge.analyte.notnull()]

    # ## units null
    # keerukas loogika
    merge.loc[
        (merge.unit.isnull() & merge.analyte.notnull())
        & (
            merge.parameter_name.str.contains("%")
            | merge.parameter_name.str.contains("suhtarv")
            | merge.parameter_code.str.contains("%")
            | merge.parameter_code.str.contains("suhtarv")
        ),
        "unit",
    ] = "%"
    merge.loc[
        (merge.unit.isnull() & merge.analyte.notnull())
        & (
            merge.parameter_name.str.contains("#")
            | merge.parameter_name.str.contains(" arv")
            | merge.parameter_code.str.contains("#")
            | merge.parameter_code.str.contains(" arv")
        ),
        "unit",
    ] = "#"
    # ## property
    merge.loc[(merge.parameter_name.fillna("").str.contains("settekiirus")), "property"] = "Vel"
    is_leuko = (
        (merge.analyte == "Neut")
        | (merge.analyte == "Baso")
        | (merge.analyte == "Eo")
        | (merge.analyte == "Lymph")
        | (merge.analyte == "Mono")
    )
    merge.loc[
        is_leuko & ((merge.unit == "%") | merge.parameter_name.fillna("").str.contains("%")), "property"
    ] = "NCnc"
    merge.loc[
        is_leuko
        & (
            (merge.unit == "#")
            | (merge.unit == "10E9/L")
            | (merge.unit == "/nL")
            | merge.parameter_name.fillna("").str.contains("#")
            | merge.parameter_name.fillna("").str.contains("absoluut")
            | merge.parameter_name.fillna("").str.contains("abs arv")
            | merge.parameter_name.fillna("").str.contains("abs.")
        ),
        "property",
    ] = "Num"
    merge.loc[(merge.unit == "pH") | (merge.parameter_name == "pH"), "property"] = "LsCnc"
    merge.loc[(merge.analyte == "RDW"), "property"] = None
    merge.loc[(merge.analyte == "RDW") & ((merge.unit == "%")), "property"] = "Ratio"
    merge.loc[(merge.analyte == "RDW") & (merge.unit == "fL"), "property"] = "EntVol"
    return merge


def to_float(n):
    try:
        return float(n)
    except:
        return None


def intersect(df, conds):
    i_res = []
    for k, v in conds.items():
        i_res.append(getattr(df, k) == (v))
    i_result = i_res[0]
    for i in i_res[1:]:
        i_result = i_result & i
    return i_result


def find_repeating(results, columns):
    sets = {}

    for k, v in results.items():
        sets[k] = set(tuple(i) for i in v.values)
    sets = list(sets.values())

    exclude = set()

    for a, b in itertools.combinations(sets, 2):
        exclude.update(a.intersection(b))

    named_excludes = [{k: v for k, v in zip([i for i in columns if i != "count"], i)} for i in exclude]
    return named_excludes


def drop_repeating(results, named_excludes):
    for i in named_excludes:
        for k in results:
            results[k] = results[k][~intersect(results[k], i)]
    return results


def load_rules(path, source_df, rule, columns) -> pd.DataFrame:
    results = {}

    functions = {
        "equals": lambda df, column, value: df[column] == (value),
        "endswith": lambda df, column, value: df[column].str.endswith(value),
        "startswith": lambda df, column, value: df[column].str.startswith(value),
    }

    directory = pathlib.Path(__file__).parent.as_posix()
    abspath = os.path.join(directory, path)

    data = json.load(open(abspath, "r"))
    for name, rules in data.items():
        rules = rules["rules"]
        ors = []

        for k, v in rules.items():
            for kk, vv in v.items():
                for i in vv:
                    ors.append(functions[kk](source_df[rule], k, i))

        if ors:
            result = ors[0]
            for i in ors[1:]:
                result = result | (i)
            results[name] = source_df[rule][result]
        else:
            print(name)

    named_excludes = find_repeating(results, columns)
    results = drop_repeating(results, named_excludes)

    for k, v in results.items():
        v["analyte"] = k

    result = pd.concat(results.values())  # type: pd.DataFrame
    return result


def main(prefix):
    directory = pathlib.Path(__file__).parent.as_posix()

    # huvitavad veerrud on
    columns = ["analysis_name", "parameter_code", "parameter_name", "unit", "count"]

    # võetakse vere analüüdid, võetakse ainult kaks analyysi tyypi
    blood = pd.DataFrame.from_csv(os.path.join(directory, "generated/blood.csv"))[columns]

    urine = pd.DataFrame.from_csv(os.path.join(directory, "generated/urine.csv"))[columns]

    # # ## Removing duplicate classes
    #
    # bloodset = set(tuple(i) for i in blood.values)
    # urineset = set(tuple(i) for i in urine.values)
    #
    # sets = [bloodset, urineset]
    #
    # exclude = set()
    #
    # # ei tee midagi...
    #
    # for a, b in itertools.combinations(sets, 2):
    #     exclude.update(a.intersection(b))
    # named_excludes = [{k: v for k, v in zip([i for i in
    #                                          columns if i != 'count'
    #                                          ], i)} for i in exclude]
    # # named_exclude on tühi....
    #
    # for i in named_excludes:
    #     blood = blood[~intersect(blood, i)]
    #     urine = urine[~intersect(urine, i)]
    #
    # # jälle ei midagi...

    # loinci süsteemi tüüp pannakse paika
    blood["system_type"] = "blood"
    urine["system_type"] = "urine"
    df = pd.concat([blood, urine])  # type: pd.DataFrame

    ######################
    # #Analytes

    blood_analytes = load_rules("splitting_blood_analytes/rules.json", df, df.system_type == "blood", columns)
    urine_analytes = load_rules("splitting_Urine_analytes/rules.json", df, df.system_type == "urine", columns)

    # siin rakendatakse mõningaid reegleid, mida võiks pigem teha "apply_rules" meetodis
    # andmete parandus, aru saada, mida teeb, jupyteris
    merge = create_merged_table(columns, df, urine_analytes, blood_analytes)

    # # Eksperdiga

    # valitakse välja sagedased (hektel pole vaja)
    active = merge[merge["count"] >= 10]
    active = active.fillna("")

    # rakendatakse keerukaid reegleid
    # property määramiseks, hetkel on active = merge
    active = apply_rules(active)

    blood_classes = load_rules("splitting_blood_classes/rules.json", df.fillna(""), df.system_type == "blood", columns)

    blood_classes.columns = list(blood_classes.columns)[:-1] + ["system"]

    # tühikud noneks lihtsalt
    blood_classes.loc[blood_classes.parameter_code == "", "parameter_code"] = None
    blood_classes.loc[blood_classes.parameter_name == "", "parameter_name"] = None
    blood_classes.loc[blood_classes.analysis_name == "", "analysis_name"] = None
    blood_classes.loc[blood_classes.unit == "", "unit"] = None
    blood_classes.loc[blood_classes.system_type == "", "system_type"] = None
    blood_classes.loc[blood_classes.system == "", "system"] = None

    # müstiline, aru saada mis teeb
    blood_classes.system.unique()

    # andmete ülekandmine
    merged_dataframe = active.merge(
        blood_classes,
        how="left",
        on=["analysis_name", "parameter_code", "parameter_name", "unit", "count", "system_type"],
        indicator=True,
    )

    merged_dataframe.loc[
        merged_dataframe.system.isnull() & (merged_dataframe.system_type == "blood"), "system"
    ] = "Bld"
    merged_dataframe.loc[
        merged_dataframe.system.isnull() & (merged_dataframe.system_type == "urine"), "system"
    ] = "Urine"

    loinc_mapping = merged_dataframe[
        ["analysis_name", "parameter_code", "parameter_name", "unit", "system_type", "analyte", "property", "system"]
    ]
    loinc_mapping.to_csv("loinc_mapping")
    # siiamaani hetkel teha ära
    # hiljem kirjutada koodi lihtsamaks

    # # DATABASE
    targets = "analyte	property	system".split()
    inputs = "analysis_name	parameter_code	parameter_name	unit".split()

    targets_inputs = [
        ({k: i[k] for k in inputs}, {k: i[k] for k in targets})
        for i in merged_dataframe[targets + inputs].to_dict(orient="records")
    ]

    engine = sq.create_engine(database_connection_string)
    connection = engine.connect()
    meta = sq.MetaData(bind=engine, schema=WORK_SCHEMA)
    meta.reflect(views=True)

    sql = """
     create view {schema}.{prefix}_analysis_mini as
     select id,analysis_name, analysis_substrate, parameter_code, parameter_name, unit
     from
         (SELECT {prefix}_analysis.id,
         {prefix}_analysis.epi_id,
             CASE
                 WHEN (({prefix}_analysis.code_system_name)::text = 'Anal��sid'::text) THEN 'Analüüsid'::character varying
                 ELSE {prefix}_analysis.code_system_name
             END AS code_system_name,
         {prefix}_analysis.analysis_name_raw AS analysis_name,
         {prefix}_analysis.analysis_substrate_raw AS analysis_substrate,
         {prefix}_analysis.parameter_code_raw AS parameter_code,
         {prefix}_analysis.parameter_name_raw AS parameter_name,
         {prefix}_analysis.reference_values,
         {prefix}_analysis.reference_values_type,
         {prefix}_analysis.effective_time,
         {prefix}_analysis.value,
             CASE
                 WHEN (({prefix}_analysis.parameter_unit IS NOT NULL) AND ({prefix}_analysis.value_unit IS NOT NULL)) THEN ((({prefix}_analysis.parameter_unit)::text || ' | '::text) || ({prefix}_analysis.value_unit)::text)
                 WHEN (({prefix}_analysis.parameter_unit IS NULL) AND ({prefix}_analysis.value_unit IS NOT NULL)) THEN ({prefix}_analysis.value_unit)::text
                 WHEN (({prefix}_analysis.parameter_unit IS NOT NULL) AND ({prefix}_analysis.value_unit IS NULL)) THEN ({prefix}_analysis.parameter_unit)::text
                 ELSE NULL::text
             END AS unit,
         {prefix}_analysis.value_type,
         {prefix}_analysis.loinc_code,
         {prefix}_analysis.loinc_name,
         {prefix}_analysis.loinc_unit
        FROM {schema}.{prefix}_analysis
       WHERE ({prefix}_analysis.value IS NOT NULL)) as foo
    """.format(
        prefix=prefix, schema=WORK_SCHEMA
    )

    # muudetud kood analysis_mini genereerimiseks
    try:
        connection.execute(sq.sql.text(sql))
    except sq.exc.ProgrammingError as e:
        pass

    print(WORK_SCHEMA, prefix)
    meta.reflect(views=True)
    view = meta.tables["{schema}.{prefix}_analysis_mini".format(prefix=prefix, schema=WORK_SCHEMA)]
    # view = meta.tables['analysiscleaning.analysis_mini']
    table = meta.tables["{schema}.{prefix}_analysis".format(prefix=prefix, schema=WORK_SCHEMA)]
    # table = meta.tables['analysiscleaning.analysis']

    # tulemuste arvutamine
    queries = []
    for target, inputs in targets_inputs:
        # print("target inputs", target)
        # selectib välja mini-st id, kus
        # SELECT analysiscleaning.analysis_mini.id
        # FROM analysiscleaning.analysis_mini, analysiscleaning.analysis
        # WHERE analysiscleaning.analysis_mini.analysis_name = % (analysis_name_1)s
        # AND analysiscleaning.analysis_mini.parameter_code = % (parameter_code_1)s
        # AND analysiscleaning.analysis_mini.parameter_name = % (parameter_name_1)s
        # AND analysiscleaning.analysis_mini.unit = % (unit_1)s

        select_query = sq.select(columns=[view.c.id], from_obj=table).where(
            sq.and_(*list(view.c.get(k) == v for k, v in target.items()))
        )
        # print(select_query)
        queries.append(select_query)
    print(queries)
    return

    # bar = tqdm.tqdm(total=len(queries))

    # tulemuste import
    # lisab analysis tabelile lõppu uued read
    for (target, inputs), query in zip(targets_inputs, queries):
        sq.update(table).where(table.c.id.in_(query)).values(
            loinc_analyte=inputs["analyte"], loinc_system=inputs["system"], loinc_property=inputs["property"]
        ).execute()
        print(inputs["analyte"])
        print(inputs["property"])
        # bar.update()


main("r07")

#
#
#
# for target, inputs in targets_inputs:
#     table.update().where(
#         sq.and_(*list(table.c.get(k) == v for k, v in target.items()))
#     ).values(
#         loinc_analyte=inputs['analyte'],
#         loinc_property=inputs['property'],
#         loinc_system=inputs['system']
#     ).execute()
#
#
# queries = []
# for target, inputs in targets_inputs:
#     select_query = sq.select(
#         columns=[table.c.value], from_obj=table).where(
#         sq.and_(*list(table.c.get(k) == v for k, v in target.items()))
#     )
#     queries.append(select_query)
#
# bar = tqdm.tqdm(total=len(queries))
# results = []
# for i in queries:
#     result = list(i.execute())
#     results.append(result)
#     bar.update()
#
# results = [[i.value for i in j] for j in results]
#
# data = []
# for d, (targ, inp) in zip(results, targets_inputs):
#     for val in d:
#         data.append({**targ, **inp, **{'value': val}})
#
# df = pd.DataFrame.from_records(data)
#
# # DF ON kõige tähtsam
# df['fixed_values'] = [to_float(i) for i in df.value]
