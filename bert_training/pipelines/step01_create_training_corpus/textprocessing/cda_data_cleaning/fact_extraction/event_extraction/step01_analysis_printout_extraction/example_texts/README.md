


# Analysis in procedures text field

Sometimes `original.procedures` column `text` contains information about blood analysis. Different types of those analysis texts are described below.

# Type1 
## Kliiniline vereanalüüs

Files text1_1-text1_6 in example_texts folder.

Starts with row `Kuupäev HK Hinnakirja kood` or `________Kuupäev HK Hinnakirja kood`. Before analysis values is row `<date> Kliiniline veri` or `<date> <5 digit number> Kliiniline veri` or `Hematoloogilised ja uriini uuringud`.

Example
```
						Kuupäev HK Hinnakirja kood
03.12.2013  URIIN
Erikaal: 1.025; Atset.: -; Valk: 100   ; Suhkur: -  ; Urobil.: -; Bilir:  ; Sade: -                   ; Reakts.: 5.0     ; Leukots.:         ; bakterid: +       ; Erütrotsüüdid -

03.12.2013  Kliiniline veri
RBC <väärtus>; HCT <väärtus>; MCV <väärtus> ; HGB <väärtus>; MCH <väärtus>; MCHC <väärtus>; PLT <väärtus>; MPV <väärtus>; WBC <väärtus>; SR   0; Gly     ; CRV     ; Valem:; keppt.  0; segmt.  0; eos.    0; basof.  0; lümf.   0; monot.  0
```

# Type 2
## Analüüside tellimus

Contains analysis names:
* `Hemogramm viieosalise/5-osalise leukogrammiga`
* `Vereäige mikroskoopia`
* `Uriini ribaanalüüs`
* `Uriini sademe mikroskoopia`
In examples mainly use `Hemogramm`, but it can be any of the analysis names given above.

* General form
```
<kuupäev> <ANONYM .../>  
ANALÜÜSIDE TELLIMUS nr: <number>  
MATERJAL: 
IP0013541173 vB <date time> (võetud:  <date time> ) 
IP0013541171 vB  <date time> (võetud:  <date time> )  
YY00958485  <date time> (võetud:  <date time> )
V102024968011  <date time>  (võetud: <date time> )  
IP0013705574 Veri (helesinises)  <date time>  (võetud:  <date time> ) 
VASTUSED: 
<optional:analysis_name>
<all analysis on seperate rows>
<optional:analysis_name>
<all analysis on seperate rows>
```


More detailed examples of different variations are described below.




* text2_0
```
<date> <ANONYM .../>  
ANALÜÜSIDE TELLIMUS nr: <number>  
VASTUSED: 
Hemogramm viieosalise/5-osalise leukogrammiga
<all analysis on seperate rows>
```

* text2_1

```
<kuupäev> <ANONYM .../>  
ANALÜÜSIDE TELLIMUS nr: <number>  
MATERJAL: 
IP0016252291 vB <date time> (võetud: <date time>) 
VASTUSED: 
Hemogramm viieosalise/5-osalise leukogrammiga
<all analysis on seperate rows>

...

ANALÜÜSIDE TELLIMUS nr: <number>  
MATERJAL: 
IP0016252283 vB <date time> (võetud: <date time>)  
IP0016252295 Veri (helesinises) <date time> (võetud: <date time>) 
VASTUSED: 
<all analysis on seperate rows>

...
ANALÜÜSIDE TELLIMUS nr: <number>  
MATERJAL: 
IP0016252294 Uriin <date time> (võetud: <date time>)
VASTUSED: 
Uriini ribaanalüüs   
<all analysis on seperate rows>
```

* text2_2
```
<date> <ANONYM .../>  
ANALÜÜSIDE TELLIMUS nr: <number>  
MATERJAL: 
KL0000125433 Uriin <date time> (võetud: <date time>) 
VASTUSED: 
Uriini ribaanalüüs   
<all analysis on seperate rows>
```


* text2_3
```
<date>
VASTUSED:
Hemogramm
<all analysis on seperate rows>

```


* text2_4

```
<date>
VASTUSED:
<all analysis on seperate rows>

```

* text2_5

```
VASTUSED:
<all analysis on seperate rows>

```

* text 2_6

```
<date>
VASTUSED:
<Hemogramm>
<all analysis on seperate rows>
Hemogramm viieosalise/5-osalise leukogrammiga  
<all analysis on seperate rows>

```

* text2_7

```
VEREANALÜÜSIDE VASTUSED: 
<all analysis on seperate rows>

```

* text2_8

```
<date time> - <ANONYM...>
ANALÜÜSIDE TELLIMUS nr: <number>
Hemogramm viieosalise leukorgammiga
WBC 3.90 (3,5 .. 8,8 E9/L )  
...

```



* text2_9

```
<kuupäev> VASTUSED: 
Hemogramm   
<all analysis on seperate rows>

```
* text2_10

```
17.12.2011 ANALÜÜSIDE TELLIMUS nr: <number> 
MATERJAL: 
V101109239546 <date time> (võetud: <date time>) 
Märkus: lima,spermatozoide 
VASTUSED:
Uriini ribaanalüüs
```

* text2_11

```
ANALÜÜSIDE TELLIMUS nr: <number>MATERJAL:YY00803671 <datte time> (võetud: <date time>)Märkus: lima+VASTUSED:Uriini analüüs testribaga<analysis on the SAME row>
```


* text2_12

```
ANALÜÜSIDE TELLIMUS nr: <7 digit number> 
MATERJAL: 
V101109239569 <date time> (võetud:  <date time> ) 
VASTUSED: 
Vereäige mikroskoopia Töös   
Hemogramm viieosalise leukogrammiga   
<all analysis on seperate rows>

```

# Type 3
## Vertical  bars

Table organized between vertical bars. Column number varies. In the header row,  first column is usually `Uuring`, last column is `Mõõtühik` and intermediate columns are dates. In some cases the header row is missing.
Contain studies:
* `Happe-aluse tasakaalu`
* `Hemogramm viieosalise leukogrammiga`
* `Mikrobioloogiline uuring`
* `Uriini sademe mikroskoopia`
* `Uriini ribaanalüüs`

--
* text3_1, 5 columns
```
<date> <random long text>
|Uuring|03.03|07.03|12.03|Mõõtühik| 
|Aeroobide uuring| |Positiivne| | | 
|Alaniini aminotransferaas seer...|18| | |U/L| 
|Anaeroobide uuring| |Puudub anaeroobne ka...| | | 
|Aspartaadi aminotransferaas se...|25| | |U/L| 
....
```

* text3_2, 3 columns
```
<kuupäev> ...
|Uuring|18.03|Mõõtühik| 
|Aeroobide uuring|Puudub aeroobne kasv| | 
|C-reaktiivne valk seerumis/plasmas|2.9|mg/L| 
|Glükoos seerumis/plasmas|6.2|mmol/L| 
|Hemogramm viieosalise leukogrammiga| | | 
|BASO#|0.04|E9/L| 
|BASO%|0.5|%| 
|EO#|0.13|E9/L|
```

* text3_3, no header row
```
|AB0-veregrupi ja Rh(D) kinnita...| | | 
|AB0 veregrupp|B| | 
|RhD veregrupp|Positiivne| | 
|Aktiveeritud osalise trombopla...|35.4|sek| 
|Amülaas (pankreasespetsiifilin...|31|U/L|  
```

* text3_4

Missing column names in the beginning of the file	
```
|Aeroobide uuring|Puudub aeroobne kasv| | | 
|Alaniini aminotransferaas seer...| |20|U/L| 
|Albumiini ja kreatiniini suhe ...|1.1| |g/mol| 
```

* text3_5, many date columns
```
|Uuring|20.12.2013|21.12.2013|22.12.2013|23.12.2013|24.12.2013|25.12.2013|26.12.2013|27.12.2013|28.12.2013|01.01|03.01|05.01|Mõõtühik| 
|Aeroobide uuring| | | |Puudub aeroobne kasv| | | | | | | | | | 
|Aktiveeritud osalise trombopla...| | | | | | | | | | |27.8| |sek|
```

* text3_6, spaces before/after uuring
```
|              
            Uuring 
|28.03|Mõõtühik| 
|Beeta-2-glükoproteiin 1 vastas...|Negatiivne| | 
```

# Type 4
## Analysis with header, single value on one row

* text4_1
Header :` Proovi võtmise kuupäev  Teostamise kuupäev  Uuring  Lühend
Tulemus  Ühik  Referents  HK kood  `
* contains `Hemogramm` or `Uriini ribaanalüüs` or `Happe-aluse tasakaal `

```
________Kuupäev HK Hinnakirja kood

...

Proovi v
õtmise kuupäev  Teostamise kuupäev  Uuring  Lühend
Tulemus  Ühik  Referents  HK kood  
08.02.2016
22:35   08.02.2016 22:35   Hemogramm  B-CBC  66201
           Leukotsüüdid veres 10E9/l    8,0
10E9/l   4-10      
```


* text4_2

```
Näita Referentsväärtusi
ja ühikuid
Analüüsid

<ANONYM.../>
alüüs

Parameeter

Vastus (08.05.2015 12:3
4:00) 

Vastus (08.05.2015 12:38:00) 

 Naatri
um seerumis/plasmas*   145  
 Hemogramm viieosali
se leukogrammiga*     
 Uurea paastuseerumis/paas
tuplasmas*   5.2  
```


* text4_3
Header : `Tellimise kuupäev  Teostamise kuupäev
  Uuring  Lühend  Tulemus  Ühik  Referents  HK koo
d  ` 

```

	Kuupäev HK Hinnakirja kood

...


Tellimise kuupäev  Teostamise kuupäev
  Uuring  Lühend  Tulemus  Ühik  Referents  HK koo
d  
<kuupäev> <kuupäev> <kellaaeg>  FT4 Vaba türok
siin seerumis  S-fT4  12,45  pmol/l   9-20   66706
   
...
```

* text4_4
	- `parameter_name`,  `reference_value`,  `parameter_unit`, `value`  
	- Exactly 1 of that type of rows exist.
```
Analüüsid 
 
Kliiniline vere ja glükoh
emoglobiini analüüs Rerentsväärtus <kellaaeg, kuupäev>
Materjal   
RBC M4,5-6,0; N:4,0-5,5 x 10*1
2/l 4,69 
...
```

# Type 5
## Analysis with header, multiple values on one row

* text5_1
Header : `
Analüüsid  
Analüüs Ühik Referents `
* contains `Hemogramm` or `Uriini ribaanalüüs` or `Vereäige mikroskoopia` or `Happe-aluse tasakaal `

```

________Kuupäev HK Hinnakirja kood

...

Analüüsid  
Analü
üs Ühik Referents 26.09.2014 23.09.2014 22.09.2014
21.09.2014 20.09.2014 
B-Gluc 11.00 Glükoos täis
veres (glükomeetriga) mmol/l - 11,0 9,5 8,8 6,8
...
B-CBC Hemogramm 
Leukotsüüdid veres 10E9/l 10E9/l 4-10
8.0 
...
Happe-aluse tasakaal (pH uriinis) Mõõtühik puudu
b 4,5-8     6     
...
U-Strip Uriini ribaanalüüs 
     Erikaal (uri
in) Mõõtühik puudub 1,002-1,03     1,010     

```


* text5_2
Header : `Analüüsid  
Analü
üs Ühik Referents <kuupäev> <kuupäev> <kuupäev> <kuupäev> <kuupäev> ` 
```

________Kuupäev HK Hinnakirja kood

...

Analüüsid  
Analü
üs Ühik Referents <kuupäev> <kuupäev> <kuupäev> <kuupäev> <kuupäev>  
B-Gluc 11.00 Glükoos täis
veres (glükomeetriga) mmol/l - 11,0 9,5 8,8 6,8
...
```


* text5_3
	* Possible analysis names:
		* `Vere automaatuuring leukogrammita `
		*  `Uriini sademe mikroskoopia `
		* `Uriini analüüs 10-parameetrilise testribaga `
	* 1 row in `original.procedures`.
```
Analüüsid  
Analüüs \ Tellitud Ühik Refer
ents <kuupäev>  <kuupäev>  <kuupäev>  <kuupäev>  <kuupäev> 

B-CBC Vere automaatuuring leukogrammita 
     L
eukotsüüdid veres 10E9/l 10E9/l   4-10 8.7   9.6 1
2.2 9.9 
...
```


* text5_4
	- `parameter_name`, `reference_value`, `parameter_unit` , `value` 

``` 
Analüüsid 
...
Biokeemia analüüs Rer
entsväärtus <date time> <date time> 
S-ALAT (alaniini aminotransferaas) N: <31 U/L,
 M:<41 U/L  12 

```


# Type 6 
## Date and analysis_name
 
Header:
1.  `Analüüsid ` or `<kuupäev> Analüüsid` or `Analüüsid <kuupäev>`
2. `<analysis name> (<date time>)` or `<date time> <analysis name>`

Possible analysis names
*`Kliiniline veri`
*`Veregrupi määramine`
*`Uriin`
*`Vere biokeemia `
*`Uriini analüüs testribaga `
*`Uriini sademe mikroskoopia  `
*`Hemogramm`
*`<ANONYM...>`

Examples 
* text6_1
	- `parameter_name`,  `value` , `reference_value`,  `parameter_unit` 
``` 
<kuupäev> ANALÜÜSID 
Triglütseriidid paastuseerumis 1.2 (<2.0 mmol/L)  
... 
Hemogramm viieosalise leukogrammiga  
WBC 7.83 (4,5 .. 10,4 E9/L)   
...
Uriini ribaanalüüs   
pH uriinis 5.0 (5.0 .. 8.0 )   
...
```

* text6_2
	- `parameter_name`,  `value` , `reference_value`,  `parameter_unit` 
``` 
ANALÜÜSID <kuupäev>
WBC 7.01 (3,5 .. 8,8 E9/L)   
...

```

* text6_3

```
<date>
hemogramm viieosalise/5-osalise leukogrammiga 
<all analysis on seperate rows>

```

* text6_4

```
<date>
hemogramm
WBC 7.50 (3,5 .. 8,8 E9/L )  
...
```

* text6_5

```
<date>
S,P-Uurea 3.3 (&lt;8.3 mmol/L ) 
...
```



* text6_6

```
<date time> - <ANONYM...>
Uriini analüüs testribaga  
SG 1.022 (1,015 .. 1,030 )  
....
Vereäige mikroskoopiline uuring  
Eosinofiilid 1.5 (1 .. 6 % )  
...
```

* text6_7 (same text file as 2_12)

```
<date time> - <ANONYM...>
P-Glükoos 5.6 (mmol/L )  
S,P-CRP 242 (&lt;5 mg/L )  
...
<date time> - <ANONYM...>
erütrotsüütide settekiirus analüsaatoril 49 (&lt;19 mm/h )  
hemogramm viieosalise leukogrammiga  
WBC 9.68 (3,5 .. 8,8 E9/L )  
...
```

* text6_8
```
Kliiniline veri: (02.02.2010 12:16)
WBC 9.07 (3,5 .. 8,8 E9/L ) 
...
Veregrupi määramine (02.02.2010 12:19)
Erütrotsütaarsete antikehade sõeluuring kahe erütrotsüüdiga Ei ole avastatud 
AB0 veregrupp geeltehnikaga A 
Rh(D) veregrupp geeltehnikaga Negatiivne 
...
Vere biokeemia: (02.02.2010 12:13)
fvS,fvP-Glükoos 4.4 (4.5 .. 6.0 mmol/L ) 
...
Uriin (02.02.2010 16:51)
SG 1.041 (1,015 .. 1,030 ) 
```

* text6_9
```
02.09.2010 11:42 - Hemogramm 
WBC 7.03 (3,5 .. 8,8 E9/L ) 
...
02.09.2010 11:49 Uriini analüüs testribaga 
SG 1.009 (1,015 .. 1,030 ) 
```

* text6_10

```
________Kuupäev HK Hinnakirja kood
<kuupäev> KLIINILINE VER <ANONYM..."/>
<ANONYM/>
<kuupäev kellaaeg>
<kuupäev kellaaeg>
Hemogramm  B-CBC  66201  
           <..."/>
ukotsüüdid veres 10E9/l    6.1  10E9/l   4-10
```


* text6_11

hematoloogilised ja uriini uuringud

```
Kuupäev HK Hinnakirja kood
<kuupäev> Haiglas tehtud Haiglas tehtud uuringud
Haiglas tehtud uuringud

Hematoloogilised ja uriini
uuringud WBC(WBC) 10 l 3,8 - 10,5 20.10.2013 7,62

Hematoloogilised ja uriini uuringud RBC(RBC) 10
l 4,5 - 5,8 20.10.2013 5,2 
```

* text

```
hemogramm viieosalise leukogrammiga
WBC 5.11 (4,1 .. 9,4 E9/L)   
...
```

# Type 7
## Table without header

Table starts straight with analytes and does not contain any header.
Tables can start with
* `S,P-Na`
* `WBC`
* `S,P Glükoos`

and end with
* `PLT`
* `S,P-CRP`
* `S,P-Kreatiniin`
* `Protorombiini`
* `S,P-CA`
* `RDW-CV`
* `fS,fP-Triglütseriidid`

* text7_1
```
WBC 11.12 (3,5 .. 8,8 E9/L )
...
PLT 248 (145 .. 390 E9/L )
S,P-Glükoos 5.1 (mmol/L )
...
S,P-CRP 56 (<5 mg/L )
S,P-Na 141 (136 .. 145 mmol/L )
...
S,P-Kreatiniin 72 (62 .. 106 µmol/L )
```

* text7_2 

```
WBC 17.00 (3,5 .. 8,8 E9/L )
...
PLT 247 (145 .. 390 E9/L ) 
RDW-CV 16.5 (11,6 .. 14 % )  
...
NEUT# 12.20 (2 .. 7 E9/L )
...
S,P-Na 136 (136 .. 145 mmol/L )
...
S,P-Kreatiniin 56 (44 .. 80 µmol/L )
Patsiendi vanus 49 (aastat ) 

```

* text7_3
```
S,P-Glükoos 4.5 (mmol/L )
...
S,P-Kreatiniin 888 (44 .. 80 µmol/L )
Patsiendi vanus 61 (aastat ) 
```

* text7_4
```
-
S,P-Glükoos 6.8 (mmol/L )
...
S,P-CRP 2 (<5 mg/L )
S,P-Na 141 (136 .. 145 mmol/L )
...
fS,fP-Triglütseriidid 1.07 (0.45 .. 2.60 mmol/L )
...
```

* text7_5

```
S,P-Na 136 (136 .. 145 mmol/L )  
...
Protrombiini % 56 (70 .. 130 % )  
```

