from cda_data_cleaning.data_cleaning.diagnosis_data_cleaning.step01_clean_diagnosis_table.icd10 import get_icd10_data, ICD10
from legacy.utils import add_to_dict, add_to_value_dict, print_dict
from shutil import copyfile
import time


class Report:
    def __init__(self, folder):
        self.folder = folder
        self.cleaning_state_counts = {}
        self.cleaning_state_codes = {}
        self.statistical_types = {}
        self.new_statistical_type_names = {}
        self.additional_information_rows = 0
        self.cleaned_name_rows = 0
        self.cleaned_code_rows = 0
        self.all_cleaned_rows = 0
        self.all = 0
        self.all_from_clean = 0
        self.cleaned_status = [
            "exact match",
            "prefix match",
            "similar match",
            "exact match; parent code",
            "prefix match; parent code",
            "parents joined match",
            "parents joined similar match",
        ]
        self.not_cleaned_status = [
            "exact match; code mismatch",
            "prefix match; code mismatch",
            "> 1 match; code match",
            "> 1 prefix match; code match",
            "> 1 match; code mismatch",
            "> 1 prefix match; code mismatch",
            "code and name are None",
            "unknown code",
            "name sub of ICD10",
            "ICD10 sub of name",
            "parts in ICD10",
            "no match",
        ]

    def report(self, prefix):
        assert self.all_cleaned_rows == self.all_from_clean
        filename = self.folder + "/" + prefix + "_" + time.strftime("%Y%m%d-%H%M") + ".md"
        with open(filename, "w") as report_file:
            report_file.write("## CLEANING REPORT:\n")
            self.print_status_dict(report_file)
            print_dict(self, self.statistical_types, "Statistical types:", report_file)
            print_dict(self, self.new_statistical_type_names, "Previously unseen statistical type names:", report_file)
            report_file.write(
                "\n\n**Additional information rows:** "
                + str(self.additional_information_rows)
                + " (%.2f%%)" % (self.additional_information_rows / self.all * 100)
                + "\n"
            )
            report_file.write(
                "\n**Cleaned names:** "
                + str(self.cleaned_name_rows)
                + " (%.2f%%)" % (self.cleaned_name_rows / (self.all) * 100)
                + "\n"
            )
            report_file.write(
                "\n**Cleaned codes:** "
                + str(self.cleaned_code_rows)
                + " (%.2f%%)" % (self.cleaned_code_rows / self.all * 100)
                + "\n"
            )
            report_file.write(
                "\n**All cleaned:** "
                + str(self.all_cleaned_rows)
                + " (%.2f%%)" % ((self.all_cleaned_rows) / self.all * 100)
                + "\n"
            )
            report_file.write("\n**Total rows:** " + str(self.all) + "\n")

        latest_report_file = self.folder + "/latest.md"
        copyfile(filename, latest_report_file)

    def log(self, cleaned_diagnosis):
        self.all += 1
        add_to_dict(self.cleaning_state_counts, cleaned_diagnosis.cleaning_status)
        add_to_dict(self.statistical_types, cleaned_diagnosis.statistical_type)
        if not cleaned_diagnosis.statistical_type:
            add_to_dict(self.new_statistical_type_names, cleaned_diagnosis.raw_statistical_type)
        if cleaned_diagnosis.code:
            add_to_value_dict(self.cleaning_state_codes, cleaned_diagnosis.cleaning_status, cleaned_diagnosis.code)
        else:
            add_to_value_dict(self.cleaning_state_codes, cleaned_diagnosis.cleaning_status, cleaned_diagnosis.raw_code)
        if cleaned_diagnosis.additional:
            self.additional_information_rows += 1
        if cleaned_diagnosis.name:
            self.cleaned_name_rows += 1
        if cleaned_diagnosis.code:
            self.cleaned_code_rows += 1
        if cleaned_diagnosis.all_clean():
            self.all_cleaned_rows += 1
        self.all_from_clean += cleaned_diagnosis.clean

    def print_status_dict(self, file):
        new_statuses = list(
            set(self.cleaning_state_counts.keys()).difference(set(self.cleaned_status + self.not_cleaned_status))
        )

        statuses = [("Cleaned:", self.cleaned_status), ("Uncleaned:", self.not_cleaned_status), ("New:", new_statuses)]
        for name, status_dict in statuses:
            if len(status_dict) > 0:
                file.write("\n\n" + "**" + name + "**" + "\n\n")
                for status in status_dict:
                    if status in self.cleaning_state_counts:
                        file.write("\n\n" + "**" + status + "**" + "\n\n")
                        file.write(
                            "* row count: "
                            + str(self.cleaning_state_counts[status])
                            + " (%.2f%%)" % (self.cleaning_state_counts[status] / self.all * 100)
                            + "\n"
                        )
                        file.write("* different codes: " + str(len(self.cleaning_state_codes[status])) + "\n")


class Cleaner:
    def __init__(self):
        name_trie, code_dict = get_icd10_data()
        self.icd10 = ICD10(name_trie, code_dict)

    def clean(self, code, name, statistical_type):
        code = code.upper().strip() if code is not None else code
        name = name.strip() if name is not None else name

        cleaned = CleanedDiagnosis(code, name, statistical_type)

        self.clean_statistical_type(cleaned)

        if code is None and name is None:
            cleaned.cleaning_status = "code and name are None"
            cleaned.clean = False
            return cleaned
        elif code is None:
            cleaned.cleaning_status = "code is None"
            cleaned.clean = False
            return cleaned
        elif name is None:
            cleaned.cleaning_status = "name is None"
            cleaned.clean = False
            return cleaned

        self.check_for_additional(cleaned)

        res = self.icd10.query(cleaned.raw_name.lower(), cleaned.raw_code)
        if res[1] is None and cleaned.trunc_name is not None:
            res = self.icd10.query(cleaned.trunc_name.lower(), cleaned.raw_code)
        cleaned.cleaning_status = res[0]
        cleaned.code = res[1]
        cleaned.name = res[2]
        cleaned.clean = res[3]

        return cleaned

    @staticmethod
    def clean_statistical_type(in_cleaning):
        statistical_type_dict = {
            "1": ["+", "esmahaigestumine", "esmashaigestumine", "esmakordne", "esmajuht elus"],
            "2": ["korduvhaigestumine", "-", "korduv", "kordusjuht elus"],
            "3": ["0", "esialgne diagnoos"],
            "4": [None, "täpsustamata", "määramata", "t�psustamata"],
        }

        result = None
        for key in statistical_type_dict:
            if in_cleaning.raw_statistical_type in statistical_type_dict[key]:
                result = key
                break
        in_cleaning.statistical_type = result

    @staticmethod
    def check_for_additional(in_cleaning):
        if ";" in in_cleaning.raw_name:
            splitted_name = in_cleaning.raw_name.split(";")
            if len(splitted_name) >= 2:
                in_cleaning.trunc_name = splitted_name[0].strip()
                in_cleaning.additional = ";".join(splitted_name[1:]).strip()

    @staticmethod
    def check_match(in_cleaning, match):
        if match is not None and len(match) == 1:
            matched_code = match[0][0].decode("ascii").strip()
            if matched_code != in_cleaning.raw_code:
                in_cleaning.cleaning_status = "code mismatch;exact name match"
            else:
                in_cleaning.cleaning_status = "match"
                in_cleaning.name = in_cleaning.raw_name
                in_cleaning.code = in_cleaning.raw_code

    @staticmethod
    def check_matches(in_cleaning, matches):
        if len(matches) == 0:
            in_cleaning.cleaning_status = "0 matches"
        elif len(matches) > 1:
            in_cleaning.cleaning_status = "> 1 matches"
        else:
            # if matches[0][1] is None:
            #    print(matches)
            # try:
            matched_code = matches[0][1][0].decode("ascii").strip()
            # except Exception as e:
            #    print(e)
            #    print(matches, len(matches))
            #    raise Exception
            matched_name = matches[0][0].strip()
            if matched_code != in_cleaning.raw_code:
                in_cleaning.cleaning_status = "code mismatch"
            else:
                in_cleaning.cleaning_status = "code match"
            if matched_name == in_cleaning.raw_name:
                in_cleaning.cleaning_status += ";exact name match"
            else:
                in_cleaning.cleaning_status += ";partial name match"

            if in_cleaning.cleaning_status == "code match;exact name match":
                in_cleaning.code = matched_code
                in_cleaning.name = matched_name


class CleanedDiagnosis:
    def __init__(self, code, name, statistical_type):
        self.raw_code = code
        self.raw_name = name
        self.raw_statistical_type = statistical_type
        self.trunc_name = None
        self.code = None
        self.name = None
        self.additional = None
        self.cleaning_status = None
        self.statistical_type = None
        self.clean = None

    def all_clean(self):
        return self.code and self.name
