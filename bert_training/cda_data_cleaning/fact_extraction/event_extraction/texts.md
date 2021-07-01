# Description of `work.<prefix>_texts` table

|Column Name|Description| Example|
|---|---|---|
| id             | primary key                              | 948802                                                       |
| data           | EstNLTK json data with `anonym` layer    | `{"meta": {}, "text": "*01.02.2016 <ANONYM id=\"0\" type=\"per\" morph=\"_H_ sg n;_H_ sg n\"/>: Patsiendil anamnesis paroksisüsmaalne tahhükardia.Obj:Nahk ja nähtavad limaskestad puhtad.Cor:Südametoonid kiired,regulaarsed.Pulmones:Kopsudes vesikulaarne hingamiskahin.Abdomen:Kõht pehme,palpatsioonil valutu.\n*03.02.2016: <ANONYM id=\"1\" type=\"per\" morph=\"_H_ sg n;_H_ sg n\"/>*Kliinilise keemia uuringud 02.02.2016 Glükoos 6.3 mmol/L [norm 4.1 - 6.1]\nKolesterool 6.1 mmol/L [norm ... - 5.0], LDL kolesterool 4.1 mmol/l [norm ... - 3.0], Raud 8.5 mkmol/L [norm 9.0 - 30.4]eGFR 83.74 ml/min/1,73 m2 [norm 90 - ...]\nPereõde <ANONYM id=\"2\" type=\"per\" morph=\"_H_ sg n;_H_ sg el\"/>:Nõustamine eluviisi ja dieedi suhtes.Omega 3x2,Retafer tabl.x2. \n", "layers": [{"name": "anonymised", "_base": "anonymised", "spans": [[{"id": "0", "end": 65, "form": "H", "type": "per", "start": 12, "partofspeech": "sg n"}], [{"id": "1", "end": 345, "form": "H", "type": "per", "start": 292, "partofspeech": "sg n"}], [{"id": "2", "end": 644, "form": "H", "type": "per", "start": 590, "partofspeech": "sg n"}, {"id": "2", "end": 644, "form": "H", "type": "per", "start": 590, "partofspeech": "sg e"}]], "parent": null, "ambiguous": true, "attributes": ["id", "type", "form", "partofspeech"], "enveloping": null}]}`|
| epi_id         | epicrisis id                             | 51079104                                                 |
| epi_type       | epicrisis type: ambulatory or stationary | a                                                        |
| schema         | schema of original                       | original                                  |
| table          | table of original                        | anamnesis                                            |
| field          | field name of original text              | anamsum                                                |
| row_id         | row id of original                       | 154791                                                       |
| effective_time | effective time / death time              | `NULL`                                      |

# Description of `work.<prefix>_extracted_texts` view

|Column Name|Description| Example|
|---|---|---|
| id             | primary key                              | 948802                                                       |
| raw_text       | raw text                                 | `*01.02.2016 <ANONYM id="0" type="per" morph="_H_ sg n;_H_ sg n"/>: Patsiendil anamnesis paroksisüsmaalne tahhükardia.Obj:Nahk ja nähtavad limaskestad puhtad.Cor:Südametoonid kiired,regulaarsed.Pulmones:Kopsudes vesikulaarne hingamiskahin.Abdomen:Kõht pehme,palpatsioonil valutu.<br/>*03.02.2016: <ANONYM id="1" type="per" morph="_H_ sg n;_H_ sg n"/>*Kliinilise keemia uuringud 02.02.2016 Glükoos 6.3 mmol/L [norm 4.1 - 6.1]<br/>Kolesterool 6.1 mmol/L [norm ... - 5.0], LDL kolesterool 4.1 mmol/l [norm ... - 3.0], Raud 8.5 mkmol/L [norm 9.0 - 30.4]eGFR 83.74 ml/min/1,73 m2 [norm 90 - ...]<br/>Pereõde <ANONYM id="2" type="per" morph="_H_ sg n;_H_ sg el"/>:Nõustamine eluviisi ja dieedi suhtes.Omega 3x2,Retafer tabl.x2.` |
| epi_id         | epicrisis id                             | 51079104                                                 |
| epi_type       | epicrisis type: ambulatory or stationary | a                                                        |
| schema         | schema of original                       | original                                  |
| table          | table of original                        | anamnesis                                            |
| field          | field name of original text              | anamsum                                                 |
| row_id         | row id of original                       | 154791                                                       |
| effective_time | effective time / death time              | `NULL`                                      |
