# Parsing of analyses data in the format of HTML tables  
  
HTML tables containing the analyses have four basic formats with subtypes enlisted below. A dedicated parser exists for each of these HTML table subtypes. The question which parsing strategy to apply to a particular HTML table is decided based on characteristics.    

## Parse type 1: Row with many entries  
  
**Characteristics:**  
  
* There are column names **Analüüsi nimetus**, **Parameeter**, **Referents**, **Ühik** in a header  
* If there are more columns names in a header, then these are dates (**effective\_time**)  
  
**Specific cases for some epicrises:**  
  * Missing **analysis\_name** is filled: If there is one less value in a row than column names in a header, then assume that the first one (**analysis\_name**) is missing and fill it from the last row where it was not missing.     
* Has **value** with time: If there is a **value** that has time(s) in a string, then include the time in **effective\_time** and save each result of analysis in a new row.  
  
**Example epicrises:**  
  
* [185784708](https://p12.stacc.ee/egcut-epicrisis-viewer/?id=185784708): one **effective_time** column, missing **analysis_name** is filled  
* [71822343](https://p12.stacc.ee/egcut-epicrisis-viewer/?id=71822343): three **effective_time** columns, missing **analysis_name** is filled, has **value** with time  
* [126337634](https://p12.stacc.ee/egcut-epicrisis-viewer/?id=126337634): three **effective_time** columns, missing **analysis_name** is filled, has **value** with time  
* [63672319](https://p12.stacc.ee/egcut-epicrisis-viewer/?id=63672319): no **effective_time** column, missing **analysis_name** is filled  
  
**Example of parsing (based on [71822343](https://p12.stacc.ee/egcut-epicrisis-viewer/?id=71822343)):**  
  
HTML table:  
  
<table>  
              <thead>  
                <tr>  
                  <th align="left">Analüüsi nimetus</th>  
                  <th>Parameeter</th>  
                  <th>Referents</th>  
                  <th>12.10.2011</th>  
                  <th>13.10.2011</th>  
                  <th>11.10.2011</th>  
                  <th>ühik</th>  
                </tr>  
              </thead>  
              <tbody>  
                <tr>  
                  <td valign="top">C-reaktiivne valk</td>  
                  <td>S-CRP</td>  
                  <td>&lt;10</td>  
                  <td>-</td>  
                  <td>-</td>  
                  <td>6</td>  
                  <td>mg/L</td>  
                </tr>  
                <tr>  
                  <td rowspan="11" valign="top">Hemogramm</td>  
                  <td>HCT</td>  
                  <td>36-46 &gt;=60 &lt;=20 &gt;=60</td>  
                  <td>-</td>  
                  <td>33.1(08:54),30.3(11:24),</td>  
                  <td>-</td>  
                  <td>%</td>  
                </tr>  
                <tr>  
                  <td>P-LCR</td>  
                  <td>-</td>  
                  <td>-</td>  
                  <td>---(08:54),38.6(11:24),</td>  
                  <td>-</td>  
                  <td>%</td>  
                </tr>  
              </tbody>  
</table>  
  
  
  
Parsed table:  
  
  
|analysis_name|parameter_name|reference_values|effective_time|value|parameter_unit|row_nr|  
|---|---|---|---|---|---|---|  
|C-reaktiivne valk|S-CRP|<10|11.10.2011|6|mg/L|1|  
|Hemogramm|HCT|36-46 >=60 <=20|13.10.2011 08:54|33.1|%|2|  
|Hemogramm|HCT|36-46 >=60 <=20|13.10.2011 11:24|30.3|%|2|  
|Hemogramm|P-LCR|None|13.10.2011 11:24|38.6|%|3|  
  
<br/>  
<br/>  
  
# Parse type 2: Row with a single entry  
  
**Characteristics:**  
  
* Header consits of two rows, where under **Tulemused** are **Tulemus** and **Kuupäev**  
* The flattended header is one of the following with the exact order:  
    * **Analüüs**, **Parameeter**, **Ühik**, **Kuupäev**, **Tulemus** (Type 2.1)  
    * **Analüüs**, **Parameeter**, **Ühik**, **Referentsväärtus**, **Tulemus**, **Kuupäev** (Type 2.2)  
    * **Analüüs**, **Parameeter**, **Ühik**, **Referentsväärtus**, **Kuupäev**, **Tulemus** (Type 2.3)    
    * **Analüüs, Ühik, Referentsväärtus, Tulemused, Kuupäev, Tulemus**   (Type 2.4)

<br/>  
  
  
## Parse type 2.1: Row without reference value   
**Table header:**  
  
<table width = "400">  
                     <thead>  
                        <tr>  
                           <th rowspan="2">Analüüs</th>  
                           <th rowspan="2">Parameeter</th>  
                           <th rowspan="2">Ühik</th>  
                           <th colspan="2">Tulemused</th>  
                        </tr>  
                        <tr>  
                           <th>Kuupäev</th>  
                           <th>Tulemus</th>  
                        </tr>  
                     </thead>  
</table>  
  
  
**Example epicrises:**  
  
* [17571635](https://p12.stacc.ee/egcut-epicrisis-viewer/?id=17571635): Type 2.1  
  
**Example of type 2.1 parsing (based on [17571635](https://p12.stacc.ee/egcut-epicrisis-viewer/?id=17571635)):**  
  
HTML table:  
  
<table>  
                     <thead>  
                        <tr>  
                           <th rowspan="2">Analüüs</th>  
                           <th rowspan="2">Parameeter</th>  
                           <th rowspan="2">Ühik</th>  
                           <th colspan="2">Tulemused</th>  
                        </tr>  
                        <tr>  
                           <th>Kuupäev</th>  
                           <th>Tulemus</th>  
                        </tr>  
                     </thead>  
                     <tbody>  
                        <tr>  
                           <td>Alkohol ja ravimid</td>  
                           <td>Digoksiin</td>  
                           <td>ng/ml</td>  
                           <td>29.01.2010</td>  
                           <td>&lt; (0,5)</td>  
                        </tr>  
                        <tr>  
                           <td>Biokeemilised uuringud</td>  
                           <td>fs-Gluc</td>  
                           <td>mmol/l</td>  
                           <td>27.01.2010</td>  
                           <td>4,92</td>  
                        </tr>  
                        <tr>  
                           <td>Biokeemilised uuringud</td>  
                           <td>B-Gluc</td>  
                           <td>mmol/l</td>  
                           <td>01.02.2010</td>  
                           <td>4,7</td>  
                        </tr>  
                     </tbody>  
</table>  
  
  
Parsed table:  
  
  
|analysis_name|parameter_name|reference_values|effective_time|value|parameter_unit|row_nr|  
|---|---|---|---|---|---|---|  
|Alkohol ja ravimid|Digoksiin| |29.01.2010|< (0,5)|ng/ml|1|  
|Biokeemilised uuringud|fs-Gluc| |27.01.2010|4,92|mmol/l|2|  
|Biokeemilised uuringud|B-Gluc| |01.02.2010|4,7|mmol/l|3|  
  
<br/>  
<br/>  
  
## Parse type 2.2: Row with a reference value  
  
**Table header:**  
  
<table width = "400">  
                     <thead>  
                        <tr>  
                           <th rowspan="2">Analüüs</th>  
                           <th rowspan="2">Parameeter</th>  
                           <th rowspan="2">Ühik</th>  
                           <th rowspan="2">Referentsväärtus</th>  
                           <th colspan="2">Tulemused</th>  
                        </tr>  
                        <tr>  
                           <th>Tulemus</th>  
                           <th>Kuupäev</th>  
                        </tr>  
                     </thead>  
</table>  
  
**Specific cases for some epicirises:**  
  
* Type 2.2 only **effective_time** in a row: If there is only one non-empty value in a row, then it is **effective_time**.  
* Type 2.2 only **effective_time** and **analysis_name** in a row: If there are two non-empty values in a row, these are **effective_time** and probably **analysis_name**. It is not known how many rows below are about this **analysis_name**.  
* Type 2.2 shifted values: If **Parameeter** is empty, then assume it is saved under **Analüüs**. Currently not used because it is better to not repair it here but instead in later steps since these are in wrong column also in the **analysis_entry** table.  
  
**Example epicrises:**  
  
* [197437418](https://p12.stacc.ee/egcut-epicrisis-viewer/?id=197437418): Type 2.2, only **effective_time** in a row, shifted values  
* [189673288](https://p12.stacc.ee/egcut-epicrisis-viewer/?id=189673288): Type 2.2, only **effective_time** in a row, shifted values  
* [91549438](https://p12.stacc.ee/egcut-epicrisis-viewer/?id=91549438): Type 2.2, only **effective_time** and **analysis_name** in a row, shifted values  
* [146543318](https://p12.stacc.ee/egcut-epicrisis-viewer/?id=146543318): Type 2.2, shifted values  
* [205677988](https://p12.stacc.ee/egcut-epicrisis-viewer/?id=205677988): Type 2.2, shifted values  
  
**Example of type 2.2 parsing (based on [91549438](https://p12.stacc.ee/egcut-epicrisis-viewer/?id=91549438)):**  
  
HTML table:  
<table>  
                     <thead>  
                        <tr>  
                           <th rowspan="2">Analüüs</th>  
                           <th rowspan="2">Parameeter</th>  
                           <th rowspan="2">Ühik</th>  
                           <th rowspan="2">Referentsväärtus</th>  
                           <th colspan="2">Tulemused</th>  
                        </tr>  
                        <tr>  
                           <th>Tulemus</th>  
                           <th>Kuupäev</th>  
                        </tr>  
                     </thead>  
                     <tbody>  
<tr><td>Hemogramm</td>  
<td/>  
<td/>  
<td/>  
<td/>  
<td>16.08.2001</td></tr>  
<tr><td>WBC</td>  
<td/>  
<td/>  
<td>3.5-8.8</td>  
<td>9.8</td>  
<td>16.08.2001</td></tr>  
<tr><td>RBC</td>  
<td/>  
<td/>  
<td>3.9-5.2</td>  
<td>4.76</td>  
<td>16.08.2001</td></tr>  
<tr><td>HGB</td>  
<td/>  
<td/>  
<td>117-153</td>  
<td>142</td>  
<td>16.08.2001</td></tr>  
<tr><td>HCT</td>  
<td/>  
<td/>  
<td>35-46</td>  
<td>44</td>  
<td>16.08.2001</td></tr>  
<tr><td>MCV</td>  
<td/>  
<td/>  
<td>82-98</td>  
<td>92.6</td>  
<td>16.08.2001</td></tr>  
<tr><td>S,P-Glükoos</td>  
<td/>  
<td/>  
<td/>  
<td>7.2</td>  
<td>16.08.2001</td></tr>  
<tr><td>Kolesterool</td>  
<td/>  
<td/>  
<td>3.9-7.8</td>  
<td>4.0</td>  
<td>16.08.2001</td></tr>  
</table>  
  
Parsed table without moving values of **Analüüs** to **Parameeter**:  
  
|analysis_name|parameter_name|effective_time|value|parameter_unit|reference_values|row_nr|  
|---|---|---|---|---|---|---|  
|WBC| |16.08.2001|9.8| |3.5-8.8|2|  
|RBC| |16.08.2001|4.76| |3.9-5.2|3|  
|HGB| |16.08.2001|142| |117-153|4|  
|HCT| |16.08.2001|44| |35-46|5|  
|MCV| |16.08.2001|92.6| |82-98|6|  
|MCH| |16.08.2001|29.8| |27-33|7|  
|MCHC| |16.08.2001|322| |317-357|8|  
|PLT| |16.08.2001|194| |145-390|9|  
|S,P-Glükoos| |16.08.2001|7.2| | |10|  
|Kolesterool| |16.08.2001|4| |3.9-7.8|11|  
  
Parsed table when moving values of **Analüüs** to **Parameeter**:  
  
|analysis_name|parameter_name|effective_time|value|parameter_unit|reference_values|row_nr|  
|---|---|---|---|---|---|---|  
| |WBC|16.08.2001|9.8| |3.5-8.8|2|  
| |RBC|16.08.2001|4.76| |3.9-5.2|3|  
| |HGB|16.08.2001|142| |117-153|4|  
| |HCT|16.08.2001|44| |35-46|5|  
| |MCV|16.08.2001|92.6| |82-98|6|  
| |MCH|16.08.2001|29.8| |27-33|7|  
| |MCHC|16.08.2001|322| |317-357|8|  
| |PLT|16.08.2001|194| |145-390|9|  
| |S,P-Glükoos|16.08.2001|7.2| | |10|  
| |Kolesterool|16.08.2001|4| |3.9-7.8|11|  
  
<br/>  
<br/>  
  
## Parse type 2.3: Row with a reference value  
  
**Table header:**  
  
<table width = "400">  
                     <thead>  
                        <tr>  
                           <th rowspan="2">Analüüs</th>  
                           <th rowspan="2">Parameeter</th>  
                           <th rowspan="2">Ühik</th>  
                           <th rowspan="2">Referentsväärtus</th>  
                           <th colspan="2">Tulemused</th>  
                        </tr>  
                        <tr>  
                           <th>Kuupäev</th>  
                           <th>Tulemus</th>  
                        </tr>  
                     </thead>  
</table>  
  
**Specific cases for some epicirises:**  
  
* Type 2.3 shifted values: If there is one more column name than value in a row, then the value under **Referentsväärtus** is actually **effective_time**, the value under **Kuupäev** is **value** and **reference_values** is missing.  
   
  
**Example epicrises:**  
  
* [82763728](https://p12.stacc.ee/egcut-epicrisis-viewer/?id=82763728): Type 2.3  
* [41090388](https://p12.stacc.ee/egcut-epicrisis-viewer/?id=41090388): Type 2.3, shifted values  
* [35332508](https://p12.stacc.ee/egcut-epicrisis-viewer/?id=35332508): Type 2.3, shifted values  
  
  
  
**Example of type 2.3 parsing (based on [41090388](https://p12.stacc.ee/egcut-epicrisis-viewer/?id=41090388)):**  
  
HTML table:  
<table>  
                     <thead>  
                        <tr>  
                           <th rowspan="2">Analüüs</th>  
                           <th rowspan="2">Parameeter</th>  
                           <th rowspan="2">Ühik</th>  
                           <th rowspan="2">Referentsväärtus</th>  
                           <th colspan="2">Tulemused</th>  
                        </tr>  
                        <tr>  
                           <th>Kuupäev</th>  
                           <th>Tulemus</th>  
                        </tr>  
                     </thead>  
                     <tbody>  
                        <tr>  
                           <td>Mikrobioloogilised uuringud</td>  
                           <td>Cm-Ctr-PCR(Chlamydia trachomatis, PCR (emakakaelakanalikaapest))</td>  
                           <td/>  
                           <td>02.03.2001</td>  
                           <td>negatiivne</td>  
                        </tr>  
                        <tr>  
                           <td>Mikrobioloogilised uuringud</td>  
                           <td>Cm-Ngo-PCR(Neisseria gonorrhoeae, PCR (emakakaelakanalikaapest))</td>  
                           <td/>  
                           <td>02.03.2001</td>  
                           <td>negatiivne</td>  
                        </tr>  
                        <tr>  
                           <td>Mikrobioloogilised uuringud</td>  
                           <td>Cf-MhUu(M.hominis´e ja U.urealyticum´i külv (emakaelaeritisest))</td>  
                           <td/>  
                           <td>02.03.2001</td>  
                           <td>Negatiivne</td>  
                        </tr>  
                     </tbody>  
                  </table>  
  
Parsed table:  
  
|analysis_name|parameter_name|effective_time|value|parameter_unit|reference_values|row_nr|  
|---|---|---|---|---|---|---|  
|Mikrobioloogilised uuringud|Cm-Ctr-PCR(Chlamydia trachomatis, PCR (emakakaelakanalikaapest))|02.03.2001|negatiivne| | |1|  
|Mikrobioloogilised uuringud|Cm-Ngo-PCR(Neisseria gonorrhoeae, PCR (emakakaelakanalikaapest))|02.03.2001|negatiivne| | |2|  
|Mikrobioloogilised uuringud|Cf-MhUu(M.hominis´e ja U.urealyticum´i külv (emakaelaeritisest))|02.03.2001|Negatiivne| | |3|  
  
<br/>  
<br/>  

  
## Parse type 2.4  

* Header is missing column **Parameeter**  
* [Test case 020](https://git.stacc.ee/project4/cda-data-cleaning/blob/master/tests/data_cleaning/analysis_html_parsing/testcase_data/testcase020.html), table 0   
* Similar to Uku's code parse type "type 2.2 shifted values"   
  - comment in the existing code: "If **Parameeter** is empty, then assume it is saved under **Analüüs**.  
     Currently not used because it is better to not repair it here but instead in later steps since these are in wrong column also in the analysis_entry table.  
  
### Example epicrisis  
* epi_id 53015838  
  
### Table header  
  
<table>  
    <thead>  
        <tr>  
            <th rowspan="2">Analüüs</th>  
            <th rowspan="2">Ühik</th>  
            <th rowspan="2">Referentsväärtus</th>  
            <th colspan="2">Tulemused</th>  
        </tr>  
        <tr>  
            <th>Kuupäev</th>  
            <th>Tulemus</th>  
        </tr>  
    </thead>  
</table>  
  
### HTML table (based on 53015838):  
  
<table>  
    <thead>  
        <tr>  
            <th rowspan="2">Analüüs</th>  
            <th rowspan="2">Ühik</th>  
            <th rowspan="2">Referentsväärtus</th>  
            <th colspan="2">Tulemused</th>  
        </tr>  
        <tr>  
            <th>Kuupäev</th>  
            <th>Tulemus</th>  
        </tr>  
    </thead>  
    <tbody>  
        <tr>  
            <td>a1178 Hemogramm 5-osalise leukogrammiga</td>  
            <td/>  
            <td/>  
            <td>08.06.2015</td>  
            <td/>  
        </tr>  
        <tr>  
            <td>a2034 WBC</td>  
            <td>E9/L</td>  
            <td>3.5 .. 8.8</td>  
            <td>08.06.2015</td>  
            <td>8.8</td>  
        </tr>  
        <tr>  
            <td>a2035 RBC</td>  
            <td>E12/L</td>  
            <td>4.20 .. 5.70</td>  
            <td>08.06.2015</td>  
            <td>5.42</td>  
        </tr>  
        <tr>  
            <td>a2041 Hb</td>  
            <td>g/L</td>  
            <td>134 .. 170</td>  
            <td>08.06.2015</td>  
            <td>177</td>  
        </tr>  
        <tr>  
            <td>a2042 Hct</td>  
            <td>%</td>  
            <td>40 .. 50</td>  
            <td>08.06.2015</td>  
            <td>52</td>  
        </tr>  
    </tbody>  
</table>  
  
### Parsed table of type 2.4 (based on 53015838):  
  
| row_nr|epi_id|epi_type|parse_type|panel_id|analysis_name_raw|parameter_name_raw|parameter_unit_raw|reference_values_raw|effective_time_raw|value_raw|analysis_substrate_raw|  
|---|---|---|---|---|---|---|---|---|---|---|---|  
|1|53015838|a|2.4|200|a1178 Hemogramm 5-osalise leukogrammiga|a2034 WBC|E9/L|3.5 .. 8.8|2015-06-08 00:00:00.000000|8.8|-|  
|2|53015838|a|2.4|200|a1178 Hemogramm 5-osalise leukogrammiga|a2035 RBC|E12/L|4.20 .. 5.70|2015-06-08 00:00:00.000000|5.42|-|  
|3|53015838|a|2.4|200|a1178 Hemogramm 5-osalise leukogrammiga|a2041 Hb|g/L|134 .. 170|2015-06-08 00:00:00.000000|177|-|  
|4|53015838|a|2.4|200|a1178 Hemogramm 5-osalise leukogrammiga|a2042 Hct|%|40 .. 50|2015-06-08 00:00:00.000000|52|-|

  
  
# Parse type 3: Packed row with a single entry  
  
**Characteristics:**  
  
* The first column name in a header is not **Analüüsi nimetus** or **Analüüs** and is a not date column. We assume it is name of analysis (e.g. Immuunmeetoditel uuringud).  
* The second column name in a header is **Rerentsväärtus**  
* All other (non-empty) column names are dates  
* Can contain more than one table, we assume each one is about one **analysis_name** type.  
  
**Specific cases for some epicrises:**  
  
* Has substrate: If in the first row and first column is **Materjal**, then the substrate of analysis is parsed from the first row of date columns.  
  
**Example epicrises:**  
  
* [77916268](https://p12.stacc.ee/egcut-epicrisis-viewer/?id=77916268): one table, has substrate  
* [26992474](https://p12.stacc.ee/egcut-epicrisis-viewer/?id=26992474): three tabels, has substrate  
* [28833902](https://p12.stacc.ee/egcut-epicrisis-viewer/?id=28833902): two tabels, no substrate  
  
**Example of parsing (based on [77916268](https://p12.stacc.ee/egcut-epicrisis-viewer/?id=77916268)):**  
  
HTML table:  
<table>  
              <thead>  
                <tr>  
                  <th>Immuunmeetoditel uuringud</th>  
                  <th>Rerentsväärtus</th>  
                  <th>24.01.2001</th>  
                </tr>  
              </thead>  
              <tbody>  
                <tr>  
                  <td>Materjal</td>  
                  <td/>  
                  <td>seerum</td>  
                </tr>  
                <tr>  
                  <td>gx1 /sp IgE heintaimede segu)</td>  
                  <td>&lt; 0,35 kU/l neg</td>  
                  <td>1.81</td>  
                </tr>  
                <tr>  
                  <td>s / sp IgE inhalats.allerg.seg</td>  
                  <td>&lt; 0,35 kU/l neg</td>  
                  <td>2.41</td>  
                </tr>  
                <tr>  
                  <td>tx5 /sp IgE puude allerg segu</td>  
                  <td>&lt; 0,35 kU/l neg</td>  
                  <td>2.06</td>  
                </tr>  
              </tbody>  
            </table>  
  
Parsed table:  
  
|analysis_name|parameter_name|effective_time|value|parameter_unit|reference_values|substrate|row_nr|  
|---|---|---|---|---|---|---|---|  
|Immuunmeetoditel uuringud|gx1 /sp IgE heintaimede segu)|24.01.2001|1.81| |< 0,35 kU/l neg|seerum|1|  
|Immuunmeetoditel uuringud|s / sp IgE inhalats.allerg.seg|24.01.2001|2.41| |< 0,35 kU/l neg|seerum|2|  
|Immuunmeetoditel uuringud|tx5 /sp IgE puude allerg segu|24.01.2001|2.06| |< 0,35 kU/l neg|seerum|3|


  
## Parse type 4  
  
* The header is the following with the exact order: **Analüüs, Parameeter, Referentsväärtus, otsustuspiir ja ühik, Proovi võtmise aeg, Tulemus, Kirjeldus**  
* Column **Kirjeldus** goes to log file  
  
### Example epicrisis  
* epi_id 52746443  
* Test case 063, table 0  
  
### Table header  
<table>  
    <thead>  
        <tr>  
            <th>Analüüs</th>  
            <th>Parameeter</th>  
            <th>Referentsväärtus, otsustuspiir ja ühik</th>  
            <th>Proovi võtmise aeg</th>  
            <th>Tulemus</th>  
            <th>Kirjeldus</th>  
        </tr>  
    </thead>  
</table>  
  
### HTML table (based on 52746443):   


<table>
    <thead>
        <tr>
            <th>Analüüs</th>
            <th>Parameeter</th>
            <th>Referentsväärtus, otsustuspiir ja ühik</th>
            <th>Proovi võtmise aeg</th>
            <th>Tulemus</th>
            <th>Kirjeldus</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td rowspan="7">Hemogramm 3 osalise leukogrammiga</td>
            <td/>
            <td/>
            <td rowspan="7" />
            <td/>
            <td rowspan="7">Hindaja: &lt;ANONYM id="7" type="per" morph="_Y_ ?;_S_ sg g"/&gt; Hindaja asutus: Quattromed HTI &lt;ANONYM id="8" type="per" morph="_H_ sg g"/&gt; labor (11107913) Analüüsi aeg: 14.03.2016 07:41</td>
        </tr>
        <tr>
            <td>Hb</td>
            <td>136 - 163</td>
            <td>10.03.2016 00:00</td>
            <td>131</td>
        </tr>
        <tr>
            <td>Hct</td>
            <td>40.0 - 54.0</td>
            <td>10.03.2016 00:00</td>
            <td>37.4</td>
        </tr>
        <tr>
            <td>Keskm suurusega leukotsüütide %</td>
            <td>2.0 - 17.0</td>
            <td>10.03.2016 00:00</td>
            <td>13.3</td>
        </tr>
        <tr>
            <td>Keskm suurusega leukotsüütide abs arv</td>
            <td>0.2 - 0.8</td>
            <td>10.03.2016 00:00</td>
            <td>0.8</td>
        </tr>
        <tr>
            <td>Lymph%</td>
            <td>20.0 - 45.0</td>
            <td>10.03.2016 00:00</td>
            <td>22.2</td>
        </tr>
        <tr>
            <td>MCH</td>
            <td>28.0 - 36.0</td>
            <td>10.03.2016 00:00</td>
            <td>34.1</td>
        </tr>
        <tr>
            <td>CRP</td>
            <td/>
            <td>&lt;10</td>
            <td/>
            <td>8.05</td>
            <td>Hindaja: &lt;ANONYM id="9" type="per" morph="_Y_ ?;_S_ sg g"/&gt; Hindaja asutus: Quattromed HTI &lt;ANONYM id="10" type="per" morph="_H_ sg g"/&gt; labor (11107913) Analüüsi aeg: 11.03.2016 11:39</td>
        </tr>
        <tr>
            <td>Kreatiniin</td>
            <td/>
            <td>59 - 104</td>
            <td/>
            <td>70</td>
            <td>Hindaja: &lt;ANONYM id="11" type="per" morph="_Y_ ?;_S_ sg g"/&gt; Hindaja asutus: Quattromed HTI &lt;ANONYM id="12" type="per" morph="_H_ sg g"/&gt; labor (11107913) Analüüsi aeg: 11.03.2016 11:39</td>
        </tr>
        <tr>
            <td>eGFR</td>
            <td/>
            <td>&gt;90</td>
            <td/>
            <td>89.70</td>
            <td>Hindaja: &lt;ANONYM id="13" type="per" morph="_Y_ ?;_S_ sg g"/&gt; Hindaja asutus: Quattromed HTI &lt;ANONYM id="14" type="per" morph="_H_ sg g"/&gt; labor (11107913) Analüüsi aeg: 11.03.2016 11:39</td>
        </tr>
        <tr>
            <td>Naatrium</td>
            <td/>
            <td>135 - 145</td>
            <td/>
            <td>141</td>
            <td>Hindaja: &lt;ANONYM id="15" type="per" morph="_Y_ ?;_S_ sg g"/&gt; Hindaja asutus: Quattromed HTI &lt;ANONYM id="16" type="per" morph="_H_ sg g"/&gt; labor (11107913) Analüüsi aeg: 11.03.2016 11:39</td>
        </tr>
    </tbody>
</table>
  
### Parsed table of type 4 (based on 52746443):  
  
| row_nr|epi_id|epi_type|parse_type|panel_id|analysis_name_raw|parameter_name_raw|parameter_unit_raw|reference_values_raw|effective_time_raw|value_raw|analysis_substrate_raw|  
|---|---|---|---|---|---|---|---|---|---|---|---|  
1|52746443|s|4.0|449|Hemogramm 3 osalise leukogrammiga|Hb||136 - 163|2016-03-10 00:00:00.000000|131|-  
2|52746443|s|4.0|449|Hemogramm 3 osalise leukogrammiga|Hct||40.0 - 54.0|2016-03-10 00:00:00.000000|37.4|-  
3|52746443|s|4.0|449|Hemogramm 3 osalise leukogrammiga|Keskm suurusega leukotsüütide %||2.0 - 17.0|2016-03-10 00:00:00.000000|13.3|-  
4|52746443|s|4.0|449|Hemogramm 3 osalise leukogrammiga|Keskm suurusega leukotsüütide abs arv||0.2 - 0.8|2016-03-10 00:00:00.000000|0.8|-  
5|52746443|s|4.0|449|Hemogramm 3 osalise leukogrammiga|Lymph%||20.0 - 45.0|2016-03-10 00:00:00.000000|22.2|-  
6|52746443|s|4.0|449|CRP|||<10||8.05|-  
7|52746443|s|4.0|449|Kreatiniin|||59 - 104||70|-  
8|52746443|s|4.0|449|eGFR|||>90||89.70|-  
9|52746443|s|4.0|449|Naatrium|||135 - 145||141|-


## Parse type 5

**Characteristics**:

* Header contains name **Tulemuse tõlgendus**.
* The header is one of the following with the exact order:
	* **Analüüs**, **Referentsväärtus, otsustuspiir ja ühik**, **Proovi võtmise aeg**, **Tulemus**, **Tulemuse tõlgendus**, **Kirjeldus** (Type 5.1)
	* **Analüüs**, **Parameeter**, **Referentsväärtus, otsustuspiir ja ühik**, **Proovi võtmise aeg**, **Tulemus**, **Tulemuse tõlgendus**, **Kirjeldus**  (Type 5.2)
	* **Analüüs**, **Parameeter**, **Referentsväärtus, otsustuspiir ja ühik**, **Proovi võtmise aeg**, **Tulemus**, ** Tulemuse tõlgendus**, **Soetud tulemus**  (Type 5.3)
	* All information from columns **Tulemuse tõlgendus**, **Soetud tulemus**, **Kirjeldus** goes to table **analysis_html_log**.

## Parse type 5.1: Row without  parameter and with "kirjeldus"

**Table header:**  
<table width = "400">  
<thead>
                                <tr>
                                    <th>Analüüs</th>
                                    <th>Referentsväärtus, otsustuspiir ja ühik</th>
                                    <th>Proovi võtmise aeg</th>
                                    <th>Tulemus</th>
                                    <th>Tulemuse tõlgendus</th>
                                    <th>Kirjeldus</th>
                                </tr>
                            </thead>
</table>  



**Example of type 5.1 parsing:**
HTML table:  
<table>
                            <thead>
                                <tr>
                                    <th>Analüüs</th>
                                    <th>Referentsväärtus, otsustuspiir ja ühik</th>
                                    <th>Proovi võtmise aeg</th>
                                    <th>Tulemus</th>
                                    <th>Tulemuse tõlgendus</th>
                                    <th>Kirjeldus</th>
                                </tr>
                            </thead>
                            <tbody>
                                <tr>
                                    <td>Glükoos</td>
                                    <td>4.1-5.9  mmol/L</td>
                                    <td>25.03.19 09:11</td>
                                    <td>4.9 mmol/L</td>
                                    <td/>
                                    <td>Proovinõu ID ja materjal: IP0055575692 Venoosne veri
TTO: AS Ida-Tallinna Keskhaigla (10822068)
Hindamise aeg: 25.03.2019 15:12</td>
                                </tr>
                                <tr>
                                    <td>HBsAg</td>
                                    <td>negatiivne  </td>
                                    <td>25.03.19 09:11</td>
                                    <td>negatiivne</td>
                                    <td/>
                                    <td>Proovinõu ID ja materjal: IP0055575692 Venoosne veri
TTO: AS Ida-Tallinna Keskhaigla (10822068)
Hindamise aeg: 25.03.2019 17:14</td>
                                </tr>
                                 <tr>
                                    <td>PAPP-A</td>
                                    <td>  IU/L</td>
                                    <td>25.03.19 09:11</td>
                                    <td>1.0 IU/L</td>
                                    <td/>
                                </tr>
   </table>       

Parsed table:  
   
|analysis_name|parameter_name|reference_values|effective_time|value|parameter_unit|row_nr|  
|---|---|---|---|---|---|---|  
||Glükoos| 4.1-5.9 mmol/L|25.03.19 09:11|4.9 mmol/L||1|  
||HBsAg| negatiivne|25.03.19 09:11|negatiivne||2|  
||PAPP-A| IU/L|25.03.19 09:11|1.0 IU/L||3|  
  


## Parse type 5.2: Row with parameter and with "kirjeldus"

**Table header:**  
<table width = "400"> <thead>
                                <tr>
                                    <th>Analüüs</th>
                                    <th>Parameeter</th>
                                    <th>Referentsväärtus, otsustuspiir ja ühik</th>
                                    <th>Proovi võtmise aeg</th>
                                    <th>Tulemus</th>
                                    <th>Tulemuse tõlgendus</th>
                                    <th>Kirjeldus</th>
                                </tr>
                            </thead>
</table>  

**Example of type 5.2 parsing:**
HTML table:  
<table>
                            <thead>
                                <tr>
                                    <th>Analüüs</th>
                                    <th>Parameeter</th>
                                    <th>Referentsväärtus, otsustuspiir ja ühik</th>
                                    <th>Proovi võtmise aeg</th>
                                    <th>Tulemus</th>
                                    <th>Tulemuse tõlgendus</th>
                                    <th>Kirjeldus</th>
                                </tr>
                            </thead>
                            <tbody>
                                <tr>
                                    <td rowspan="6">FP-KORVETISTE-PAKETT</td>
                                    <td>FP-PEPS_1-AK</td>
                                    <td>30.0 - 160.0 µg/l</td>
                                    <td>20181029083300</td>
                                    <td>91.0</td>
                                    <td>N</td>
                                    <td rowspan="6">Proovinõu ID ja materjal: 546461-01 Plasma 
Hindaja: Meeli Glükmann(D05979)
Eriala:laborimeditsiin(E190) 
TTO: synlab Eesti OÜ (11107913) 
Hindamise aeg: 01.11.18 16:30</td>
                                </tr>
                                <tr>
                                    <td>FP-PEPS_2-AK</td>
                                    <td>3.0 - 15.0 µg/l</td>
                                    <td>20181029083300</td>
                                    <td>13.1</td>
                                    <td>N</td>
                                </tr>
                                <tr>
                                    <td>FP-PEPS_1_2-AK</td>
                                    <td>3.00 - 20.00 </td>
                                    <td>20181029083300</td>
                                    <td>6.95</td>
                                    <td>N</td>
                                </tr>
                                <tr>
                                    <td>FP-GASTR-AK</td>
                                    <td>1.0 - 7.0 pmol/l</td>
                                    <td>20181029083300</td>
                                    <td>1.6</td>
                                    <td>N</td>
                                </tr>
          </table>
          


Parsed table:

|analysis_name|parameter_name|reference_values|effective_time|value|parameter_unit|row_nr|  
|---|---|---|---|---|---|---|  
|FP-KORVETISTE-PAKETT|FP-PEPS_1-AK|30.0 - 160.0 µg/l|20181029083300|91.0||1|  
|FP-KORVETISTE-PAKETT|FP-PEPS_2-AK|3.0 - 15.0 µg/l|20181029083300|13.1||2|  
|FP-KORVETISTE-PAKETT|FP-PEPS_1_2-AK| 3.00 - 20.00|20181029083300|6.95||3|  
|FP-KORVETISTE-PAKETT|FP-GASTR-AK|1.0 - 7.0 pmol/l|20181029083300|1.6||4|  

## Parse type 5.3: Row with parameter and with "soetud tulemus"

**Table header:**  
<table width = "400"> 
<thead>
                                <tr>
                                    <th>Analüüs</th>
                                    <th>Parameeter</th>
                                    <th>Referentsväärtus, otsustuspiir ja ühik</th>
                                    <th>Proovi võtmise aeg</th>
                                    <th>Tulemus</th>
                                    <th>Tulemuse tõlgendus</th>
                                    <th>Soetud tulemus</th>
                                </tr>
                            </thead>
</table>  


**Example of type 5.3 parsing:**

HTML table:  

<table>
	<thead>
                                <tr>
                                    <th>Analüüs</th>
                                    <th>Parameeter</th>
                                    <th>Referentsväärtus, otsustuspiir ja ühik</th>
                                    <th>Proovi võtmise aeg</th>
                                    <th>Tulemus</th>
                                    <th>Tulemuse tõlgendus</th>
                                    <th>Soetud tulemus</th>
                                </tr>
                            </thead>
         <tbody>
<tr>
                                    <td>66102 </td>
                                    <td>S-UricA</td>
                                    <td>184.0 - 464.0  µmol/L</td>
                                    <td>20180608091118</td>
                                    <td>393</td>
                                    <td>N</td>
                                    <td/>
                                </tr>
                                <tr>
                                    <td>66112 </td>
                                    <td>S-CRP</td>
                                    <td>0.0 - 9.0  mg/L</td>
                                    <td>20180608091118</td>
                                    <td>0.26</td>
                                    <td>N</td>
                                    <td/>
                                </tr>
                                <tr>
                                    <td>66111 </td>
                                    <td>fS-RF</td>
                                    <td>0.0 - 14.0  IU/mL</td>
                                    <td>20180608091118</td>
                                    <td>26.8</td>
                                    <td>N</td>
                                    <td/>
                                </tr>
                                <tr>
                                    <td>66101 </td>
                                    <td>P-Gluc</td>
                                    <td>4.1 - 6.1  mmol/L</td>
                                    <td>20180608091118</td>
                                    <td>5.0</td>
                                    <td>N</td>
                                    <td/>
                                </tr>
                      </tbody>
</table>  

Parsed table:

|analysis_name|parameter_name|reference_values|effective_time|value|parameter_unit|row_nr|  
|---|---|---|---|---|---|---|  
|66102|S-UricA|184.0 - 464.0 µmol/L|20180608091118|393||1|  
|66112|S-CRP|0.0 - 9.0 mg/L|20180608091118|0.26||2|  
|66111|fS-RF|0.0 - 14.0 IU/mL|20180608091118|26.8||3|  
|66101|P-Gluc|4.1 - 6.1 mmol/L|20180608091118|5.0||4|  


