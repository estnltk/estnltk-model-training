import luigi

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import CDAJob


class ExportDiagTextLayersToTable(CDAJob):

    prefix: str = luigi.Parameter()
    config_file = luigi.Parameter()
    luigi_targets_folder = luigi.Parameter(default="luigi_targets")
    # role = luigi.Parameter()
    # schema = luigi.Parameter()

    def requires(self):
        return [self.requirement]

    def run(self):
        storage = self.create_estnltk_storage(self.config_file)

        # with get_postgres_storage(self.config_file) as storage:
        collection = storage[self.prefix + "_diagnosis_parsing"]

        collection.export_layer(
            layer="cancer_stages",
            attributes=["grammar_symbol", "regex_type", "value"],
            collection_meta=[
                "src_id",
                "epi_id",
                "field",
                "table",
                "collection"
            ],
            table_name="temp_" + self.prefix + "_diagnosis_parsing_cancer_stages",
        )

        collection.export_layer(
            layer="stages",
            attributes=["grammar_symbol", "regex_type", "value"],
            collection_meta=[
                "src_id",
                "epi_id",
                "field",
                "table",
                "collection"
            ],
            table_name="temp_" + self.prefix + "_diagnosis_parsing_stages",
        )

        collection.export_layer(
            layer="dates_numbers",
            attributes=["grammar_symbol", "regex_type", "value"],
            collection_meta=[
                "src_id",
                "epi_id",
                "field",
                "table",
                "collection"
            ],
            table_name="temp_" + self.prefix + "_diagnosis_parsing_dates_numbers",
        )

        collection.export_layer(
            layer="diagnosis",
            attributes=["grammar_symbol", "regex_type", "value"],
            collection_meta=[
                "src_id",
                "epi_id",
                "field",
                "table",
                "collection"
            ],
            table_name="temp_" + self.prefix + "_diagnosis_parsing_diagnosis",
        )

        storage.conn.commit()
        storage.close()

        self.mark_as_complete()
