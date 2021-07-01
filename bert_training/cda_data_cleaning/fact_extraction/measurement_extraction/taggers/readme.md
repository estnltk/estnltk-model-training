# Measurement extraction

Taggers and scripts to extract measurements.

## Taggers

### MeasurementTokenTagger

Measurement token tagger tags numbers, units, dates and other measurement components in the text and stores the result in the measurement token layer.

### MeasurementTagger

Measurement tagger uses finite grammar to find measurements from the measurement token layer and stores the result in the `measurements` layer. 

## Scripts

### extract_measurement_tokens

Run `MeasurementTokenTagger` on EstNltk PostgreSQL collection to create a measurement tokens layer.

To display details, run
```
grammarextractor$ grammarextractor/data_processing/measurement_extraction/extract_measurement_tokens -h
```
See also a [configuration file example](../../../bash_workflows/run_measurement_extraction_conf/extract_measurement_tokens.conf).

### extract_measurements_from_tokens
Run `MeasurementTagger` on EstNltk PostgreSQL collection if the measurement tokens layer already exists.

To display details, run
```
grammarextractor$ grammarextractor/data_processing/measurement_extraction/extract_measurements_from_tokens -h
```
See also a [configuration file example](../../../bash_workflows/run_measurement_extraction_conf/extract_measurements_from_tokens.conf)

 
### extract_measurements

Run `MeasurementTokenTagger` and `MeasurementTagger` on EstNltk PostgreSQL collection.

To display details, run
```
grammarextractor$ $ grammarextractor/data_processing/measurement_extraction/extract_measurements -h 
```
