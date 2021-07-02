import luigi

from cda_data_cleaning.common.luigi_tasks import CDAJob
from cda_data_cleaning.fact_extraction.common_tasks import CreateLayer
from cda_data_cleaning.fact_extraction.common.taggers.cancer_stage_tagger.cancer_stage_tagger import CancerStageTagger
from cda_data_cleaning.fact_extraction.common.taggers.diagnosis_text_tagger.diagnosis_text_tagger import DiagnosisTextTagger
from cda_data_cleaning.fact_extraction.common.taggers.diag_stage_tagger.diag_stage_tagger import DiagnosisStageTagger
from cda_data_cleaning.fact_extraction.common.taggers.robust_date_number_tagger.robust_date_number_tagger import RobustDateNumberTagger


class CreateDiagParsingLayers(CDAJob):

    prefix = luigi.Parameter()
    config_file = luigi.Parameter()
    # schema = luigi.Parameter()
    # role = luigi.Parameter()
    luigi_targets_folder = luigi.Parameter(default="luigi_targets")

    def requires(self):

        # check if 'texts' collection exists, if not then create it
        # conn = self.create_postgres_connection(self.config_file)
        # db_table_exists = self.db_table_exists(
        #     conn=conn, table=str(self.prefix) + "_diagnosis_parsing", schema=self.work_schema)

        task_01 = self.requirement
        # if not db_table_exists:
        #     print("\n\n\nDb table does not exist!")
        #     error = "Collection {schema}.{prefix}_diagnosis_parsing does not exist, first" \
        #             " run CreateDiagTextCollection and then try again!".format(
        #                 schema=self.work_schema, prefix=self.prefix
        #             )
        #     raise Exception(error)

        task_02 = CreateLayer(
            prefix=self.prefix,
            config_file=self.config_file,
            work_schema=self.work_schema,
            role=self.role,
            collection="diagnosis_parsing",
            layer="cancer_stages",
            tagger=CancerStageTagger(output_layer="cancer_stages"),
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_01,
        )

        task_03 = CreateLayer(
            prefix=self.prefix,
            config_file=self.config_file,
            work_schema=self.work_schema,
            role=self.role,
            collection="diagnosis_parsing",
            layer="diagnosis",
            tagger=DiagnosisTextTagger(output_layer="diagnosis"),
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_02,
        )

        task_04 = CreateLayer(
            prefix=self.prefix,
            config_file=self.config_file,
            work_schema=self.work_schema,
            role=self.role,
            collection="diagnosis_parsing",
            layer="stages",
            tagger=DiagnosisStageTagger(output_layer="stages"),
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_03,
        )

        task_05 = CreateLayer(
            prefix=self.prefix,
            config_file=self.config_file,
            work_schema=self.work_schema,
            role=self.role,
            collection="diagnosis_parsing",
            layer="dates_numbers",
            tagger=RobustDateNumberTagger(output_layer="dates_numbers"),
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_04,
        )

        return [task_01, task_02, task_03, task_04, task_05]

    def run(self):
        self.mark_as_complete()
