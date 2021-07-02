# Description of `work.<prefix>_events` table

|Column Name     |Type     | Description                              | Example|
|----------------|---------|------------------------------------------|----------------------------------------------------------|
| id             |bigint   | primary key                              | 1458887                                                     |
| data           |jsonb    | EstNLTK json data with `anonym` layer    | `{"meta": {}, "text": " <ANONYM id=\"1\" type=\"per\" morph=\"_H_ sg n;_H_ sg n\"/>*Kliinilise keemia uuringud 02.02.2016 Glükoos 6.3 mmol/L [norm 4.1 - 6.1]\nKolesterool 6.1 mmol/L [norm ... - 5.0], LDL kolesterool 4.1 mmol/l [norm ... - 3.0], Raud 8.5 mkmol/L [norm 9.0 - 30.4]eGFR 83.74 ml/min/1,73 m2 [norm 90 - ...]\nPereõde <ANONYM id=\"2\" type=\"per\" morph=\"_H_ sg n;_H_ sg el\"/>:Nõustamine eluviisi ja dieedi suhtes.Omega 3x2,Retafer tabl.x2. \n", "layers": [{"name": "anonymised", "_base": "anonymised", "spans": [[{"id": "1", "end": 54, "form": "H", "type": "per", "start": 1, "partofspeech": "sg n"}], [{"id": "2", "end": 353, "form": "H", "type": "per", "start": 299, "partofspeech": "sg n"}, {"id": "2", "end": 353, "form": "H", "type": "per", "start": 299, "partofspeech": "sg e"}]], "parent": null, "ambiguous": true, "attributes": ["id", "type", "form", "partofspeech"], "enveloping": null}]}`  |
| epi_id         |text     | epicrisis id                             | 51079104                                                 |
| epi_type       |text     | epicrisis type: ambulatory or stationary | a                                                        |
| schema         |text     | schema of original                       | original                                  |
| table          |text     | table of original                        | anamnesis                                         |
| field          |text     | field name of original text              | anamsum                                                  |
| row_id         |text     | row id of original text                  | 154791                                                      |
| effective_time |timestamp| effective time / death time              | `NULL`                                      |
| header         |text     | header of the event                      | `*03.02.2016:`                                       |
| header_offset  |integer  | header offset in the text                | 278                                                      |
| event_offset   |integer  | event offset in the text                 | 291                                                      |


# Description of `work.<prefix>_extracted_events` view

|Column Name     |Type     | Description                              | Example|
|----------------|---------|------------------------------------------|----------------------------------------------------------|
| id             |bigint   | `work.<prefix>_events` primary key       | 1377                                                     |
| raw_text       |text     | event text string                        | ` <ANONYM id="1" type="per" morph="_H_ sg n;_H_ sg n"/>*Kliinilise keemia uuringud 02.02.2016 Glükoos 6.3 mmol/L [norm 4.1 - 6.1]<br/>Kolesterool 6.1 mmol/L [norm ... - 5.0], LDL kolesterool 4.1 mmol/l [norm ... - 3.0], Raud 8.5 mkmol/L [norm 9.0 - 30.4]eGFR 83.74 ml/min/1,73 m2 [norm 90 - ...]<br/>Pereõde <ANONYM id="2" type="per" morph="_H_ sg n;_H_ sg el"/>:Nõustamine eluviisi ja dieedi suhtes.Omega 3x2,Retafer tabl.x2. `|
| epi_id         |text     | epicrisis id                             | 51079104                                                 |
| epi_type       |text     | epicrisis type: ambulatory or stationary | a                                                        |
| schema         |text     | schema of original                       | original                                  |
| table          |text     | table of original                        | anamnesis                                         |
| field          |text     | field name of original text              | anamsum                                                     |
| row_id         |text     | row id of original text                  | 154791                                                      |
| effective_time |text     | effective time / death time              | `NULL`                                      |
| header         |text     | header of the event                      | `*03.02.2016:`                                         |
| header_offset  |integer  | header offset in the text                | 278                                                      |
| event_offset   |integer  | event offset in the text                 | 291                                                      |
