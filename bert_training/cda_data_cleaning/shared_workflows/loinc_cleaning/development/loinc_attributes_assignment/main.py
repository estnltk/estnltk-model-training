import os
import pathlib

import luigi
import pandas as pd
import sqlalchemy as sq

from cda_data_cleaning.shared_workflows.loinc_cleaning.development.loinc_attributes_assignment import (
    loincing,
    experiments,
)
from legacy.utils import write_empty_file
from configuration import luigi_targets_folder, WORK_SCHEMA, database_connection_string

directory = pathlib.Path(__file__).parent.as_posix()


class GenerateWithCounts(luigi.Task):
    prefix = luigi.Parameter()

    def run(self):
        engine = sq.create_engine(database_connection_string)
        connection = engine.connect()
        meta = sq.MetaData(bind=engine, schema=WORK_SCHEMA)
        meta.reflect(views=True)

        sql = """
        select analysis_name, analysis_substrate, parameter_code, parameter_name, unit, count(*) count from
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
        group by analysis_name, analysis_substrate,parameter_code, parameter_name, unit;
        """.format(
            prefix=self.prefix, schema=WORK_SCHEMA
        )

        r = connection.execute(sq.sql.text(sql))

        df = pd.DataFrame.from_records(dict(i) for i in r)

        df[["analysis_name", "analysis_substrate", "parameter_code", "parameter_name", "unit", "count"]].to_csv(
            os.path.join(directory, "generated/with_counts.csv"), index=None
        )

        write_empty_file(os.path.join(luigi_targets_folder, self.prefix), self.output())

    def output(self):
        return luigi.LocalTarget(os.path.join(luigi_targets_folder, self.prefix, "GenerateWithCounts_done"))


class PrepRawDataFiles(luigi.Task):
    prefix = luigi.Parameter()

    def requires(self):
        return GenerateWithCounts(self.prefix)

    def run(self):
        # TODO: local file paths to absolutes
        loincing.main()
        write_empty_file(os.path.join(luigi_targets_folder, self.prefix), self.output())

    def output(self):
        return luigi.LocalTarget(os.path.join(luigi_targets_folder, self.prefix, "PrepRawDataFiles_done"))


class LOINC_system_analyte_property(luigi.Task):
    prefix = luigi.Parameter()

    def requires(self):
        return PrepRawDataFiles(self.prefix)

    # doesnt run anymore
    def run(self):
        experiments.main(self.prefix)
        write_empty_file(os.path.join(luigi_targets_folder, self.prefix), self.output())

    def output(self):
        return luigi.LocalTarget(os.path.join(luigi_targets_folder, self.prefix, "LOINC_system_analyte_property_done"))
