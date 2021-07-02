import json
import pathlib

import numpy as np
import os
import pandas as pd

directory = pathlib.Path(__file__).parent.as_posix()


def startswith(line, items):
    return any([line.startswith(item) for item in items])


def endswith(line, items):
    return any([line.endswith(item) for item in items])


def is_blood(row):
    if startswith(row["parameter_code"], ["B-", "B1-", "B2-"]):  # and ' ' not in row['parameter_code']:
        return True

    if startswith(row["parameter_name"], ["B-", "B1-", "B2-"]):  # and ' ' not in row['parameter_name']:
        return True

    if startswith(row["analysis_name"], ["B-", "B1-", "B2-"]):  # and ' ' not in row['analysis_name']:
        return True

    if row["analysis_name"] == "Hematoloogilised uuringud":
        return True

    if row["analysis_name"].endswith("leukogrammiga"):
        return True

    if row["analysis_substrate"] in ("täisveri", "paastuveri"):
        return True

    if row["analysis_name"] in [
        "Kliiniline vereanalüüs",
        "Kliiniline vere ja glükohemoglobiini analüüs",
        "hemogramm viieosalise leukogrammiga",
        "hemogramm",
        "hemogramm 5-osalise leukogrammiga",
        "Vere automaatuuring 5-osalise leukogrammiga",
        "Hematoloogia",
        "Hematoloogia labori analüüsid",
        "Laboratoorne hematoloogia",
    ]:
        return True

    if (
        row["analysis_name"].endswith(" veres")
        and not row["analysis_name"].endswith("arteriaalses veres")
        and not row["analysis_name"].endswith("venoosses veres")
        and not not row["analysis_name"].endswith("venooses veres")
    ):
        return True
    if row["analysis_name"].endswith("paastuveres"):
        return True

    # Erütrotsüüdid
    if row["parameter_name"] in ["rbc", "RBC", "RBC(RBC)"]:
        return True

    # Leukotsüüdid
    if row["parameter_name"] in ["wbc", "WBC", "WBC(WBC)"]:
        return True

    # Trombotsüüdid
    if row["parameter_name"] in ["plt", "PLT", "PLT(PLT)"]:
        return True

    # Hematokrit
    if row["parameter_name"] in ["hct", "HCT", "HCT(HCT)"]:
        return True

    # näitab punaliblede mahtu ehk suurust
    if row["parameter_name"] in ["mcv", "MCV", "MCV(MCV)"]:
        return True

    # näitab, kui palju sisaldab üks punalible hemoglobiini
    if row["parameter_name"] in ["mch", "MCH", "MCH(MCH)"]:
        return True

    # näitab hemoglobiini hulka kogu vere ühes liitris
    if row["parameter_name"] in ["mchc", "MCHC", "MCHC(MCHC)"]:
        return True

    # Hemoglobiin (HGB) kromoproteiin mis sisaldub erütrotsüütides
    if row["parameter_name"] in ["hgb", "HGB", "HGB(HGB)"]:
        return True

    # neutrofiilid (Neut)
    if row["parameter_name"] in ["neut%", "neut#", "neut"]:
        return True

    # eosinofiilid (EO)
    if row["parameter_name"] in ["eo%", "eo#", "eo"]:
        return True

    # basofiilid (BASO)
    if row["parameter_name"] in ["baso%", "baso#", "baso"]:
        return True

    # lümfotsüüdid (LYMPH)
    if row["parameter_name"] in ["lymph%", "lymph#", "LYMPH", "LYMPH (abs.)", "lymph", "LYMP"]:
        return True

    # monotsüüdid (MONO)
    if row["parameter_name"] in ["mono%", "mono#", "mono"]:
        return True

    # Granulotsüüdid: neutrofiilid (Neut), eosinofiilid (EO), basofiilid (BASO)
    if row["parameter_name"] in ["GRAN", "GRA (abs.)", "GRA"]:
        return True

    # agranulotsüüdid: lümfotsüüdid (LYMPH) ja monotsüüdid (MONO).
    if row["parameter_name"] in []:
        return True
    return False


def is_arterialblood(row):
    if startswith(row["parameter_code"], ["aB-"]):  # and ' ' not in row['parameter_code']:
        return True

    if startswith(row["parameter_name"], ["aB-"]):  # and ' ' not in row['parameter_name']:
        return True

    if startswith(row["analysis_name"], ["aB-"]):  # and ' ' not in row['analysis_name']:
        return True

    if row["analysis_name"] == "Areteriaalne veri":
        return True

    if row["analysis_substrate"] == "Areteriaalne veri":
        return True

    if row["analysis_name"].endswith("arteriaalses veres"):
        return True

    return False


def is_venoseblood(row):
    if startswith(row["parameter_code"], ["vB-"]):  # and ' ' not in row['parameter_code']:
        return True

    if startswith(row["parameter_name"], ["vB-"]):  # and ' ' not in row['parameter_name']:
        return True

    if startswith(row["analysis_name"], ["vB-"]):  # and ' ' not in row['analysis_name']:
        return True

    if row["analysis_name"] == "Venoosne veri":
        return True

    if row["analysis_substrate"] == "Venoosne veri":
        return True

    return False


def is_capillaryblood(row):
    if startswith(row["parameter_code"], ["cB-"]):  # and ' ' not in row['parameter_code']:
        return True

    if startswith(row["parameter_name"], ["cB-"]):  # and ' ' not in row['parameter_name']:
        return True

    if startswith(row["analysis_name"], ["cB-"]):  # and ' ' not in row['analysis_name']:
        return True

    return False


def is_urine(row):
    if startswith(row["parameter_code"], ["U-", "dU-"]):  # and ' ' not in row['parameter_code']:
        return True

    if startswith(row["parameter_name"], ["U-", "dU-"]):  # and ' ' not in row['parameter_name']:
        return True

    if startswith(row["analysis_name"], ["U-", "dU-"]):  # and ' ' not in row['analysis_name']:
        return True

    if row["parameter_name"] in ("U-SG(Uriini erikaal)"):
        return True

    if row["analysis_substrate"] in ("uriin", "juhuslik uriin", "ööpäevane uriin", "1.hommikune uriin"):
        return True

    if row["analysis_name"] == "uriini ribaanalüüs":
        return True

    if row["analysis_name"] in [
        "Uriini sademe mikroskoopia",
        "Uriini uuringud",
        "Uriini analüüs",
        "Uriini analüüs 10-param. testribaga",
        "Uriini uuring 10-parameetrilise testribaga",
        "Uriini analüüs 10-parameetrilise testribaga",
        "Uriin",
        "Uriini ribaanalüüs",
        "Uriini analüüs 10 parameetrilise testribaga",
        "uriini sademe mikroskoopia",
        "uriini analüüs testribaga",
        "Albumiin juhuslikus uriinis",
        "Kreatiniin uriinis",
        "5-hüdroksüindooläädikhape ööpäevauriinis",
        "Katehhoolamiinide metaboliidid ööpäevauriinis",
        "Katehoolamiinid ööpäevauriinis",
        "albumiin uriini",
        "uriini sade",
        "Albumiin uriini",
        "pH uriinis",
        "Koproporfüriinide ja kreatiniini suhe uriinis",
        "Albumiin uriinis",
        "Erikaal (uriin)",
    ]:
        return True

    return False


def is_serum(row):
    if endswith(row["analysis_name"], [" seerumis "]):
        return True

    if startswith(row["parameter_code"], ["S-", "S1-", "S2-", "fS-", "fs-"]):  # and ' ' not in row['parameter_code']:
        return True
    if startswith(row["parameter_name"], ["S-", "S1-", "S2-", "fS-", "fs-"]):  # and ' ' not in row['parameter_name']:
        return True

    if startswith(row["analysis_name"], ["S-", "S1-", "S2-", "fS-", "fs-"]):  # and ' ' not in row['analysis_name']:
        return True
    if row["parameter_name"] in (
        "S-AGA IgG" "S-CA 72-4(9. Kasvaja antigeen CA 72-4)",
        "S-Ig fLambda(Immuunglobuliini vabad lambdaahelad)",
        "S-AGA IgG",
    ):
        return True

    if row["analysis_substrate"] == "seerum":
        return True

    if row["analysis_name"] in [
        "Kasvajaantigeen CA 15-3 seerum",
        "Naatrium seerumis",
        "Kreatiniin seerumis",
        "<ANONYM> fraktsioonid seerumis",
        "Ekstraheeritavate tuumaantigeenide vastane IgI seerumis (paneel)",
        "<ANONYM> fraktsioonid seerumis elektroforeetiliselt",
        "Inhaleeritud allergeenispetsiifilised IgE antikehad seerumis",
        "Ekstraheeritavate tuumaantigeenide vastane IgG seerumis (paneel)",
        "Allergeenspetsiifilised IgE antikehad seerumis",
        "Suguhormoone siduv globuliin seerumis",
        "Vaba türoksiin seerumis (ELFA)",
        "Mycoplasma pneumoniae vastane IgM seerumis",
        "Amülaas seerumis",
        "C-hepatiidi viiruse RNA hulk seerumis",
        "Foolhape seerumis",
        "Immuunglobuliin G alaklassid 1-4 seerumis",
        "Kreatiniin paastuseerumis",
        "Valk seerumis (üldvalk)",
        "Raud seerumis",
        "Kartsinoembrüonaalne antigeen seerumis (ELFA)",
        "Üldine immuunoglobuliin G seerumis",
        "PSA Prostataspetsiifiline antigeen seerumis",
        "Koe transglutaminaasi vastane IgA seerumis",
        "Ferritiin seerumis",
        "Chlamydia trachomatis vastane IgG seerumis",
        "Tuumavastane IgG seerumis",
        "Glükoos seerumis",
        "Prostataspetsiifiline antigeen seerumis (ELFA)",
        "CEA Kartsinoembrüonaalne antigeen seerumis",
        "Türeotropiini retseptori vastane IgG seerumis",
        "17-OH progesteroon, vereseerumist",
        "Albumiin seerumis",
        "Antistreptolüsiin O seerumis",
        "Vaba prostataspetsiifiline antigeen seerumis",
        "C-reaktiivne valk seerumis",
        "Valk seerumis",
        "Kasvajaantigeen HE4 seerumis",
        "Kasvaja-antigeen CA 125 seerumis (ELFA)",
        "Kreatiini kinaas seerumis",
        "Beeta-2-glükoproteiin1 vastased antikehad seerumis",
        "Uurea paastuseerumis",
        "Laktaadi dehüdrogenaas seerumis",
        "Vitamiin B12 paastuseerumis",
        "Vaba türoksiin seerumis",
        "IgG tüüpi transglutaminaasi vastased antikehad vereseerumist",
        "Vabad kappa ja lambda kergahelad seerumis",
        "Chlamydophila pneumoniae vastane IgA seerumis",
        "Triglütseriidid paastuseerumis",
        "FSH Follitropiin ehk folliikuleid stimuleeriv hormoon seerumis",
        "Uurea seerumis",
        "fT4 - Vaba türoksiin seerumis",
        "Mycoplasma pneumoniae vastane IgG seerumis",
        "Alfa-amülaas seerumis",
        "Mycoplasma pneumoniae vastane IgA seerumis",
        "Chlamydophila pneumoniae vastane IgG seerumis",
        "LH Lutropiin ehk luteiniseeriv hormoon seerumis",
        "Aluseline fosfataas seerumis",
        "Kardiolipiinivastased IgG, IgM ,IgA antikehad seerumis",
        "Kasvajaantigeen S-100 seerumis",
        "Magneesium seerumis",
        "Dehüdroepiandrosteroonsulfaat seerumis",
        "HDL- kolesterool paastuseerumi",
        "Kaltsium (ioniseeritud) seerum",
        "Kusihape seerumis",
        "fPSA Vaba prostataspetsiifiline antigeen seerumis",
        "TSH Türeotropiin ehk kilpnääret stimuleeriv hormoon seerumis",
        "Türeoglobuliinivastane IgG seerumis",
        "Komplemendi komponent C3 seerumis",
        "Chlamydophila pneumoniae vastane IgM seerumis",
        "Kilpnäärme peroksüdaasi vastased autoantikehad seerumis",
        "FT4 Vaba türoksiin seerumis",
        "Immuunglobuliin E seerumis",
        "Tsüklilise tsitrulleeritud peptiidi vastane IgG seerumis",
        "Atsetüülkollini vastased antikehad seerumis",
        "Digoksiin seerumis",
        "Troponiin T seerumis",
        "Kasvaja-antigeen CA 15-3 seerumis (ELFA)",
        "Folaat paastuseerumis",
        "fT4 Vaba türoksiin seerumis",
        "Testosteroon seerumis",
        "Türeotropiin ehk kilpnääret stimuleeriv hormoon seerumis (ELFA)",
        "Prolaktiin seerumis",
        "C1 esteraasi inhibiitor seerumis",
        "Kolesterool seerumis",
        "Vaba trijoodtüroniin seerumis",
        "Kaltsium seerumis",
        "Kaalium seerumis",
        "Kaltsium paastuseerumis",
        "Aldosteroon seerumis",
        "LDL- kolesterool paastuseerumi",
        "Bilirubiin seerumis",
        "Alfa1-antitrüpsiin seerumis",
        "fT3 Vaba trijoodtüroniin seerumis",
        "Seroloogiline analüüs",
    ]:
        return True

    if row["parameter_name"].endswith(" seerumis"):
        return True

    if row["parameter_name"].endswith(" paastuseerumis"):
        return True

    return False


def is_plasma(row):
    if startswith(row["parameter_code"], ["P-", "P1-", "P2-", "fP-", "p-"]):  # and ' ' not in row['parameter_code']:
        return True
    if startswith(row["parameter_name"], ["P-", "P1-", "P2-", "fP-", "p-"]):  # and ' ' not in row['parameter_name']:
        return True

    if startswith(row["analysis_name"], ["P-", "P1-", "P2-", "fP-", "p-"]):  # and ' ' not in row['analysis_name']:
        return True

    if row["analysis_substrate"] in ("plasma",):
        return True

    if row["analysis_name"].endswith(" plasmas"):
        return True

    if row["analysis_name"].endswith(" paastuplasmas"):
        return True

    if row["parameter_name"].endswith(" plasmas"):
        return True

    if row["parameter_name"].endswith(" paastuplasmas"):
        return True

    return False


def is_sputum(row):
    if startswith(row["parameter_code"], ["Sp-"]):  # and ' ' not in row['parameter_code']:
        return True

    if startswith(row["parameter_name"], ["Sp-"]):  # and ' ' not in row['parameter_name']:
        return True

    if startswith(row["analysis_name"], ["Sp-"]):  # and ' ' not in row['analysis_name']:
        return True

    return False


def is_ser_pla(row):
    if startswith(row["parameter_code"], ["s,p-", "S,P-", "fS,fP-", "fs,fp-"]) and " " not in row["parameter_code"]:
        return True

    if startswith(row["parameter_name"], ["s,p-", "S,P-", "fS,fP-", "fs,fp-"]) and " " not in row["parameter_name"]:
        return True

    if startswith(row["analysis_name"], ["s,p-", "S,P-", "fS,fP-", "fs,fp-"]) and " " not in row["analysis_name"]:
        return True
    if row["analysis_name"].endswith(" seerumis/plasmas"):
        return True

    if row["parameter_name"].endswith(" seerumis/plasmas"):
        return True

    if row["parameter_name"].endswith(" paastuseerumis/paastuplasmas"):
        return True

    if row["parameter_name"] == "s,p-vaba t4":
        return True

    return False


def is_pericardial(row):
    if row["analysis_substrate"] == "Perikardivedelik":
        return True

    return False


def is_pleural(row):
    if row["analysis_substrate"] == "Pleuravedelik":
        return True

    if row["analysis_name"].endswith("pleuravedelikus"):
        return True

    if row["analysis_name"].endswith("pleuravedelikust"):
        return True

    if row["analysis_name"].startswith("Pleuravedeli"):
        return True

    return False


def is_CSF(row):
    if startswith(row["parameter_code"], ["CSF"]):  # and ' ' not in row['parameter_code']:
        return True

    if startswith(row["parameter_name"], ["CSF"]):  # and ' ' not in row['parameter_name']:
        return True

    if startswith(row["analysis_name"], ["CSF"]):  # and ' ' not in row['analysis_name']:
        return True

    if row["analysis_substrate"] in ("Liikvor", "SF - Liikvor"):
        return True

    if row["analysis_name"] in [
        "Liikvori analüüs",
        "Liikvori (seljaajuvedeliku) makroskoopiline või tsütoosi uuring",
        "Liikvori uuringud",
        "Liikvor",
    ]:
        return True

    return False


#####################################################################################################################################################################
#####################################################################################################################################################################
#####################################################################################################################################################################


def main():
    df = pd.read_csv(os.path.join(directory, "generated/with_counts.csv"))

    df[df.analysis_substrate.notnull()].analysis_substrate.unique()

    df["System"] = None

    df.analysis_name = df.analysis_name.astype(str)
    df.analysis_substrate = df.analysis_substrate.astype(str)
    df.parameter_code = df.parameter_code.astype(str)
    df.parameter_name = df.parameter_name.astype(str)

    r = []
    for line in df.to_dict(orient="records"):
        r.append(is_pleural(line))
    df["Pleural"] = None
    df["Pleural"] = r

    r = []
    for line in df.to_dict(orient="records"):
        r.append(is_pericardial(line))
    df["Pericardial"] = None
    df["Pericardial"] = r

    r = []
    for line in df.to_dict(orient="records"):
        r.append(is_ser_pla(line))
    df["SerPla"] = None
    df["SerPla"] = r

    r = []
    for line in df.to_dict(orient="records"):
        r.append(is_venoseblood(line))
    df["VenoseBlood"] = None
    df["VenoseBlood"] = r

    r = []
    for line in df.to_dict(orient="records"):
        r.append(is_capillaryblood(line))
    df["CapillaryBlood"] = None
    df["CapillaryBlood"] = r

    r = []
    for line in df.to_dict(orient="records"):
        r.append(is_arterialblood(line))
    df["ArterialBlood"] = None
    df["ArterialBlood"] = r

    r = []
    for line in df.to_dict(orient="records"):
        r.append(is_CSF(line))
    df["CSF"] = None
    df["CSF"] = r

    r = []
    for line in df.to_dict(orient="records"):
        r.append(is_blood(line))
    df["Blood"] = None
    df["Blood"] = r

    r = []
    for line in df.to_dict(orient="records"):
        r.append(is_plasma(line))
    df["Plasma"] = None
    df["Plasma"] = r

    r = []
    for line in df.to_dict(orient="records"):
        r.append(is_serum(line))
    df["Serum"] = None
    df["Serum"] = r

    r = []
    for line in df.to_dict(orient="records"):
        r.append(is_urine(line))

    df["Urine"] = None
    df["Urine"] = r

    r = np.zeros(shape=df["Urine"].values.size, dtype=np.uint)

    df.Blood ^= df.Blood & df.SerPla
    df.Blood ^= df.Blood & df.Serum
    df.Blood ^= df.Blood & df.Plasma
    df.Blood ^= df.Blood & df.CapillaryBlood

    df.Blood ^= df.Blood & df.CSF

    df.Blood ^= df.Urine & df.Blood
    df.Plasma ^= df.Urine & df.Plasma
    df.SerPla ^= df.Urine & df.SerPla
    df.Serum ^= df.Urine & df.Serum

    for i in (
        df["Urine"].values,
        df["Blood"].values,
        df["Serum"].values,
        df["Plasma"].values,
        df["ArterialBlood"].values,
        df["CSF"].values,
        df["CapillaryBlood"].values,
        df["VenoseBlood"].values,
        df["SerPla"].values,
        df["Pericardial"].values,
        df["Pleural"].values,
    ):
        r += i
    # missed = df[r > 1]
    mitmesed, kategooriata, yhesed = df[r == 2]["count"].sum(), df[r == 0]["count"].sum(), df[r == 1]["count"].sum()

    print(
        """mitmeseks jäid {mitmesed} rida
    kategoriseerimata jäid {kategooriata} rida
    ühesed {yhesed} rida""".format(
            **locals()
        )
    )

    bloods = df.Blood | df.SerPla | df.Serum | df.Plasma | df.ArterialBlood | df.CapillaryBlood | df.VenoseBlood
    blood = df[bloods]

    print("\nveresid on {}".format(blood["count"].sum()))
    Urine = df[df["Urine"]]["count"].sum()
    Pericardial = df[df["Pericardial"]]["count"].sum()
    Pleural = df[df["Pleural"]]["count"].sum()
    CSF = df[df["CSF"]]["count"].sum()

    print(
        """
    Urine - {Urine}
    Pericardial - {Pericardial}
    Pleural - {Pleural}
    CSF - {CSF}
    """.format(
            **locals()
        )
    )

    # Kõik, mida ma vereks pean.
    blood = df[df.Blood | df.SerPla | df.Serum | df.Plasma]

    blood.to_csv(os.path.join(directory, "generated/blood.csv"))
    df[df["Urine"]].to_csv(os.path.join(directory, "generated/urine.csv"))
    df[df["CSF"]].to_csv(os.path.join(directory, "generated/csf.csv"))

    # Kõik ühikud üle vaatamiseks.
    # TODO generated/units.csv jääb tühjaks
    df[["unit", "count"]].groupby("unit").apply(lambda x: x["count"].sum()).sort_values(by="unit").to_csv(
        os.path.join(directory, "./generated/units.csv")
    )

    #################################
    #################################
    #################################
    #################################
    df = pd.read_csv(os.path.join(directory, "generated/blood.csv"), index_col=None)

    df = df[
        [
            "analysis_name",
            "parameter_code",
            "parameter_name",
            "unit",
            "count",
            "SerPla",
            "VenoseBlood",
            "CapillaryBlood",
            "ArterialBlood",
            "Blood",
            "Plasma",
            "Serum",
        ]
    ]

    s = ["analysis_name", "parameter_code", "parameter_name", "unit"]
    i = ["count"]
    b = ["SerPla", "VenoseBlood", "CapillaryBlood", "ArterialBlood", "Blood", "Plasma", "Serum"]

    for col in s:
        df[[col]] = df[[col]].astype(str)

    for col in i:
        df[[col]] = df[[col]].astype(int)

    for col in b:
        df[[col]] = df[[col]].astype(bool)

    with open(os.path.join(directory, "splitting_blood_classes/data.json"), "w") as f:
        json.dump(
            [
                {**d, **{"index": i}}
                for d, i in zip(
                    df[["analysis_name", "parameter_code", "parameter_name", "unit", "count"]].to_dict(
                        orient="records"
                    ),
                    range(100000),
                )
            ],
            f,
        )

    ############################
    df = pd.read_csv(os.path.join(directory, "generated/urine.csv"), index_col=None)

    df = df[["analysis_name", "parameter_code", "parameter_name", "unit", "count"]]

    s = ["analysis_name", "parameter_code", "parameter_name", "unit"]
    i = ["count"]
    b = []

    for col in s:
        df[[col]] = df[[col]].astype(str)

    for col in i:
        df[[col]] = df[[col]].astype(int)

    for col in b:
        df[[col]] = df[[col]].astype(bool)

    with open(os.path.join(directory, "splitting_Urine_analytes/data.json"), "w+") as f:
        json.dump(
            [
                {**d, **{"index": i}}
                for d, i in zip(
                    df[["analysis_name", "parameter_code", "parameter_name", "unit", "count"]].to_dict(
                        orient="records"
                    ),
                    range(100000),
                )
            ],
            f,
        )


main()

#####################################
# df = pd.read_csv('generated/csf.csv', index_col=None)
#
# df = df[['analysis_name', 'parameter_code',
#          'parameter_name', 'unit', 'count']]
#
# s = ['analysis_name', 'parameter_code',
#      'parameter_name', 'unit']
# i = ['count']
# b = []
#
# for col in s:
#     df[[col]] = df[[col]].astype(str)
#
# for col in i:
#     df[[col]] = df[[col]].astype(int)
#
# for col in b:
#     df[[col]] = df[[col]].astype(bool)
#
# with open('../splitting_CSF_analytes/data.json', 'w+') as f:
#     json.dump([{**d, **{'index': i}} for d, i in zip(df[['analysis_name', 'parameter_code',
#                                                          'parameter_name', 'unit', 'count']].to_dict(orient='records'),
#                                                      range(100000)
#                                                      )], f)
