# Description of `work.<prefix>_freetext_analysis_loinced_with_text` view

This view joins `work.<prefix>_freetext_analysis_loinced`, `work.<prefix>_texts` and `work.<prefix>_events` tables.

|Column Name       |Type     | Description                              | Example|
|------------------|---------|------------------------------------------|----------------------------------------------------------|
|text_id           |integer  | `work.<prefix>_events` primary key       | 1458887                                                  |
|span_nr           |integer  | index of the measurement span in the event                 | 0                                      |
|span_start        |integer  | start of the measurement span relative to the `event_text` | 82                                     |
|span_end          |integer  | start of the measurement span relative to the `event_text` | 111                                    |
|name              |text     |                                          | MEASUREMENT                                              |
|parameter_name    |text     | object                                   | Glükoos                                                  |
|value             |text     | same as `VALUE` in `work.<prefix>_freetext_analysis_loinced`                                         | 6.3                                                      |
|unit              |text     |                                          | mmol/L                                                   |
|min               |text     | lower reference value                    |                                                          |
|max               |text     | upper reference value                    |                                                          |
|date              |text     |                                          | 02.02.2016                                               |
|regex_type        |text     |                                          | GLÜKOOS                                                  |
|loinc_code        |text     |                                          |                                                   |
|t_lyhend          |text     |                                          |                                                   |
|epi_id            |text     | epicrisis id                             | 51079104                                                 |
|epi_type          |text     | epicrisis type: ambulatory or stationary | a                                                        |
|schema            |text     | schema of original                       | original                                                 |
|table             |text     | table of original                        | anamnesis                                                |
|field             |text     | field name of original text              | anamsum                                                  |
|row_id            |text     | row id of original text                  | 154791                                                   |
|effective_time    |text     | effective time / death time              | `NULL`                                                   |
|header            |text     | header of the event                      | `*03.02.2016:`                                             |
|header_offset     |text     | header offset in the text relative to `raw_text`           | 278                                    |
|event_offset      |text     |            |                                     |
|event_header_date |text     |            |                                     |
|doctor_code       |text     |            |                                     |
|specialty         |text     |            |                                     |
|specialty_code    |text     |            |                                     |
|field_text        |text     | original text string                     | `*01.02.2016 <ANONYM id="0" type="per" morph="_H_ sg n;_H_ sg n"/>: Patsiendil anamnesis paroksisüsmaalne tahhükardia.Obj:Nahk ja nähtavad limaskestad puhtad.Cor:Südametoonid kiired,regulaarsed.Pulmones:Kopsudes vesikulaarne hingamiskahin.Abdomen:Kõht pehme,palpatsioonil valutu.<br/>*03.02.2016: <ANONYM id="1" type="per" morph="_H_ sg n;_H_ sg n"/>*Kliinilise keemia uuringud 02.02.2016 Glükoos 6.3 mmol/L [norm 4.1 - 6.1]<br/>Kolesterool 6.1 mmol/L [norm ... - 5.0], LDL kolesterool 4.1 mmol/l [norm ... - 3.0], Raud 8.5 mkmol/L [norm 9.0 - 30.4]eGFR 83.74 ml/min/1,73 m2 [norm 90 - ...]<br/>Pereõde <ANONYM id="2" type="per" morph="_H_ sg n;_H_ sg el"/>:Nõustamine eluviisi ja dieedi suhtes.Omega 3x2,Retafer tabl.x2. `|
|event_text        |text     | event text string                        |  `<ANONYM id="1" type="per" morph="_H_ sg n;_H_ sg n"/>*Kliinilise keemia uuringud 02.02.2016 Glükoos 6.3 mmol/L [norm 4.1 - 6.1]<br/>Kolesterool 6.1 mmol/L [norm ... - 5.0], LDL kolesterool 4.1 mmol/l [norm ... - 3.0], Raud 8.5 mkmol/L [norm 9.0 - 30.4]eGFR 83.74 ml/min/1,73 m2 [norm 90 - ...]<br/>Pereõde <ANONYM id="2" type="per" morph="_H_ sg n;_H_ sg el"/>:Nõustamine eluviisi ja dieedi suhtes.Omega 3x2,Retafer tabl.x2. `|
