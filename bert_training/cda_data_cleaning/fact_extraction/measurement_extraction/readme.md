# Measurement extraction

## Run measurement extraction

NB! `RunExtractMeasurementsToTable` workflow expects `ExtractEvents` workflow to be completed and will raise an
exception if it has not been run.

1. `RunExtractMeasurementsToTable`

    Extract measurements in the `events` collection and export them to a table `<prefix>_extracted_measurements`.
    ```bash
    $ luigi --scheduler-port 8082 \
   --module cda_data_cleaning.fact_extraction.measurement_extraction.run_extract_measurements_to_table RunExtractMeasurementsToTable \
   --prefix=run_202103031555 \
   --config-file=configurations/egcut_epi_microrun.example.ini \
   --workers=1
    ```

## Development

If fixing or improving the code first try out the changes in the [development](development) folder and then copy 
the changes from [development](development) to here. See [development/readme.md](development/readme.md) for details.
