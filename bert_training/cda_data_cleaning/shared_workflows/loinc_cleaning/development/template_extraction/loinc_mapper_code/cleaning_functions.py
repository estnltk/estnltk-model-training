import itertools
import json
import os
import pathlib

import numpy as np
import pandas as pd

# see fail on kummaline sest on tehtud ipython notebooki põhjal.
# vt https://git.stacc.ee/project4-egcut/LOINCtagger/blob/master/experiments.ipynb

# kas ridade antav väljaantavas vüib muutuda? jääbsamaks?
# mis veerge muudetakse? milliseid veerge eeldab olemas olevat?
def apply_property_rules(dataframe):
    """Fills dataframe property with values according to given rules.

    :param dataframe: pandas dataframe with columns analysis_name, parameter_code, parameter_name, unit, count,	system_type, analyte, property
                    most of the propertys have values NaN.
    :type dataframe: pandas.DataFrame
    :returns:  dataframe with filled property values.
    :rtype: pandas.DataFrame
    """

    dataframe = dataframe.fillna("")
    UNK = "unknown"

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
    dataframe = empty_str_to_none(dataframe)

    return dataframe


# kas none ja na sama?


def empty_str_to_none(dataframe):
    # # Splitting blood classes
    dataframe.loc[dataframe.property == "", "property"] = None
    dataframe.loc[dataframe.analyte == "", "analyte"] = None
    dataframe.loc[dataframe.parameter_code == "", "parameter_code"] = None
    dataframe.loc[dataframe.parameter_name == "", "parameter_name"] = None
    dataframe.loc[dataframe.analysis_name == "", "analysis_name"] = None
    dataframe.loc[dataframe.unit == "", "unit"] = None
    return dataframe


def empty_str_to_none_blood(blood_classes):
    blood_classes.loc[blood_classes.parameter_code == "", "parameter_code"] = None
    blood_classes.loc[blood_classes.parameter_name == "", "parameter_name"] = None
    blood_classes.loc[blood_classes.analysis_name == "", "analysis_name"] = None
    blood_classes.loc[blood_classes.unit == "", "unit"] = None
    blood_classes.loc[blood_classes.system_type == "", "system_type"] = None
    blood_classes.loc[blood_classes.system == "", "system"] = None
    return blood_classes


def apply_rules_old(dataframe):
    UNK = "unknown"

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

    # kaheks cleanup, emptystringstona-s vms
    # kas none ja na sama?
    dataframe.loc[dataframe.property == "", "property"] = None
    dataframe.loc[dataframe.analyte == "", "analyte"] = None
    dataframe.loc[dataframe.parameter_code == "", "parameter_code"] = None
    dataframe.loc[dataframe.parameter_name == "", "parameter_name"] = None
    dataframe.loc[dataframe.analysis_name == "", "analysis_name"] = None
    dataframe.loc[dataframe.unit == "", "unit"] = None
    return dataframe


def create_merged_table(columns: list, df, urine_analytes, blood_analytes):
    """Merges dataframe df with urine and blood analytes

    :param columns: is a list of columns we are interested in blood and urine analytes, 
           df: is pandas dataframe where we want to add analyte and property,
           urine_analytes: pandas DataFrame where we get analyte names for urine, 
           blood_analytes: where we get analyte names for blood.
    :type columns: list, 
          df: pandas.DataFrame, 
          urine_analytes: pandas.DataFrame, 
          blood_analytes: pandas.Dataframe.
    :returns: dataframe filled with system_type, analyte and property.
    :rtype: pandas.DataFrame.

    """
    res = {"bl": blood_analytes[columns], "ur": urine_analytes[columns]}
    # võtab välja vere analüüsid
    blood_analytes = blood_analytes[np.array([i in res["bl"].index for i in blood_analytes.index])]
    urine_analytes = urine_analytes[np.array([i in res["ur"].index for i in urine_analytes.index])]

    # paneb üksteise järele
    r = pd.concat([blood_analytes, urine_analytes])

    # table'i ühendamine isenendaga ehk join, columnsid peaavd kokku langema
    merge = pd.merge(df, r, on=columns, how="outer")
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
        i_result &= i
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

    data = json.load(open(abspath))
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


def add_bloodanalytes_systemtype(blood, columns):
    """Adds system_type and analyte columns to blood dataframe

    :param blood: initial blood dataframe
           columns: contains columns analysis_name, parameter_code, parameter_name, unit, count.
    :type blood: pandas.DataFrame
          columns: list.
    :returns dataframe where system_type is blood and analytes values are determined by json rules.
    :rtype pandas.DataFrame.
    """
    blood["system_type"] = "blood"
    blood_analytes = load_rules("splitting_blood_analytes/rules.json", blood, blood.system_type == "blood", columns)
    merge = pd.merge(blood, blood_analytes, on=columns + ["system_type"], how="outer")
    return merge


def add_urineanalytes_systemtype(urine, columns):
    urine["system_type"] = "urine"
    urine_analytes = load_rules("splitting_Urine_analytes/rules.json", urine, urine.system_type == "urine", columns)
    merge = pd.merge(urine, urine_analytes, on=columns + ["system_type"], how="outer")
    return merge


def clean_units(df):
    """Adds unit values to some rows where unit is missing according to analyte, parameter_name and parameter_code

    :param df: dataframe that contains columns analysis_name, parameter_code, parameter_name, unit, count, system_tyoe and analyte
    :type df: pandas.DataFrame,
    :returns dataframe with some corrected units
    :rtype pandas.DataFrame
    """

    df.loc[
        (df.unit.isnull() & df.analyte.notnull())
        & (
            df.parameter_name.str.contains("%")
            | df.parameter_name.str.contains("suhtarv")
            | df.parameter_code.str.contains("%")
            | df.parameter_code.str.contains("suhtarv")
        ),
        "unit",
    ] = "%"
    df.loc[
        (df.unit.isnull() & df.analyte.notnull())
        & (
            df.parameter_name.str.contains("#")
            | df.parameter_name.str.contains(" arv")
            | df.parameter_code.str.contains("#")
            | df.parameter_code.str.contains(" arv")
        ),
        "unit",
    ] = "#"
    return df


def add_property(df):
    """Adds some property values based on analytes, parameter_name and unit

    :param df: dataframe that contains columns analysis_name, parameter_code, parameter_name, unit, count, system_tyoe and analyte
    :type df: pandas.DataFrame,
    :returns dataframe with property column and some property values
    :rtype pandas.DataFrame
    """

    df.loc[(df.parameter_name.fillna("").str.contains("settekiirus")), "property"] = "Vel"
    is_leuko = (
        (df.analyte == "Neut")
        | (df.analyte == "Baso")
        | (df.analyte == "Eo")
        | (df.analyte == "Lymph")
        | (df.analyte == "Mono")
    )
    df.loc[is_leuko & ((df.unit == "%") | df.parameter_name.fillna("").str.contains("%")), "property"] = "NCnc"
    df.loc[
        is_leuko
        & (
            (df.unit == "#")
            | (df.unit == "10E9/L")
            | (df.unit == "/nL")
            | df.parameter_name.fillna("").str.contains("#")
            | df.parameter_name.fillna("").str.contains("absoluut")
            | df.parameter_name.fillna("").str.contains("abs arv")
            | df.parameter_name.fillna("").str.contains("abs.")
        ),
        "property",
    ] = "Num"
    df.loc[(df.unit == "pH") | (df.parameter_name == "pH"), "property"] = "LsCnc"
    df.loc[(df.analyte == "RDW"), "property"] = None
    df.loc[(df.analyte == "RDW") & ((df.unit == "%")), "property"] = "Ratio"
    df.loc[(df.analyte == "RDW") & (df.unit == "fL"), "property"] = "EntVol"

    return df


def create_blood_classes(blood, columns):
    """Adds system column to blood dataframe.

    :param blood: filled dataframe with blood values
           columns:list of original dataframe column names - analysis_name, parameter_code, parameter_name, unit, count.
    :type blood: pandas.DataFrame
          columns: list
    :returns: dataframe which has an extra column called system and is filled with values Bld.
    :rtype: pandas.DataFrame.
    """
    columns += ["system_type"]
    blood_classes = load_rules(
        "splitting_blood_classes/rules.json", blood.fillna(""), blood.system_type == "blood", columns
    )
    # blood_classes tekib juurde veerg nimega analyte, see muudetakse systemiks
    blood_classes.columns = list(blood_classes.columns)[:-1] + ["system"]
    blood_classes = empty_str_to_none_blood(blood_classes)
    blood_classes.system.unique()
    return blood_classes
