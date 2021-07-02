# Fact extraction

# `CreateLayer`

This is a general luigi task for tagging a detached layer `LAYER` on a collection `COLLECTION`.

The intended usage is as follows

```python
import luigi
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction import CreateLayer


class TagNewLayer(luigi.Task):
    ...

    def requires(self):
        return [CreateLayer(self.prefix, self.config_file, self.COLLECTION, self.LAYER, self.REQUIREMENT)]

    ...
```
Here `COLLECTION` is the name of the collection on which the layer is created.

The layer name `LAYER` with the corresponding tagger instance must be declared in [create_layer_conf.py](create_layer_conf.py).

The `REQUIREMENT` is a luigi task that must be done befeore running the `CreateLayer` task.

The layer is created by running luigi sub-tasks (`CreateLayerBlock`) where the number of parallel processes 
is equal to the luigi `ẁorkers` parameter.

The layer dependencies are resolved. So, for instace,

```python
import luigi
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction import
    CreateTextsCollection
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction import CreateLayer


class TagNewLayer(luigi.Task):
    ...

    def requires(self):
        return [CreateLayer(self.prefix, self.config_file, 'texts', 'event_segments',
                            CreateTextsCollection(self.prefix, self.config_file))]

    ...
```
creates the collection `texts` with the layers `event_tokens`, `event_headers` and `event_segments`.


# Luigi workflows

To run the fact extraction luigi workflows use the command

```bash
$ luigi --scheduler-port <port> --module cda_data_cleaning.fact_extraction <luigi_task> --prefix=<prefix> --conf=egcut_epi_microrun.ini --workers=<workers> --log-level=<log-level>
```

where 
* `<port>` is the luigi scheduler port number (try 8082 on a local machine or 8089 on `p12.stacc.ee`),
* `<prefix>` is any string which is valid as a PostgreSQL table name,
* `conf_file` is the name of the configurations file in the `cda-data-cleaning/configurations` folder,
* `<luigi_task>` is the name of one of the luigi tasks listed below, 
* `<workers>` is the number of luigi workers (see the luigi documentation for details),
* `<log-level>` is the luigi log level (see the luigi documentation for details).

For example
```bash
$ luigi --scheduler-port 8082 --module cda_data_cleaning.fact_extraction ExportMeasurementsToTable --prefix=microrun_201906250950 --conf=egcut_epi_microrun.ini --workers=4 --log-level=INFO
```

## Event extraction

### `ExtractEvents`

## Measurement extraction

For extracting the measurements, please refer to [measurement extraction's README](measurement_extraction/readme.md)

## Procedure splitting

### `CreateProcedureTextsCollection`

Import raw texts from the `text` column of the `procedures_entry` table in the originals schema and create a 
`<prefix>_procedure_texts` collection with attached `anonymised` layer.
   
### `CreateProcedureEntriesCollection`

Run `CreateProcedureTextsCollection`. Tag `event_tokens`, `event_headers` and `event_segments` layers on the 
`<prefix>_procedure_texts` collection. Split procedure texts by `event_segments` layer and create a new 
`<prefix>_procedure_entries` collection.

### `CreateProcedureSubentriesCollection`

Run `CreateProcedureEntriesCollection`, tag `subcategories` and `subcategory_segments` layers on the 
`<prefix>_procedure_entries` collection. Split texs in the `<prefix>_procedure_entries` collection by the
`subcategory_segments` layer and create a new `<prefix>_procedure_subentries` collection. 

### `CreateStudyLayerInProcedureSubentries`

Run `CreateProcedureSubentriesCollection`, and tag `study` layer on the `<prefix>_procedure_subentries` collection.
