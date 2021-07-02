# Measurement extraction examples

Following examples give motivation for additional tuning of measurement extractions. 
Exampples are based on EGCUT Delivery 3 data, 
from view work.run_201904181825_extracted_measurements_with_text (specific field names 
are those that are used for release r008 for EGCUT).

Further remarks and expectations are given in Estonian.

--------

event_text:

 - <ANONYM id="207" type="per" morph="_H_ sg n"/>, <ANONYM id="208" type="per" morph="_Y_ ?"/> - M00123 - N600 - ämmaemandus 
VM 08.09.2012. Anamneesis viljatus, 4 kunstliku viljastamise katset, millest 1 juhul toimus rasestumine ja peetumine. Seekord spontaanne rasedus. Anamneesis sapikivid ja sapipõie eemaldamine ning 1 lap.co. Kroonilisi haigusi ei põe. Pikkus 174 cm, kehakaal 70 kg. Enesetunne hea, aegajalt väsimust ja iiveldust. Töötab fotograafina, töö iseloomult liikuv ja suhteliselt intensiivne, fotovarusutus, mida kaasas kannab kaalub 7 kg. Nõustamine ja soovitused. Võetud emakakaela analüüsid, annab veenivered. Tagasikutse 03.12 UH-sse NT mõõtmiseks ja arvele võtmiseks.   

* value_category: 'KAAL'
* value: '7'
* value_unit: 'kg'
* epi_id: '24245099'
* source: summary.sumsum:220748 (sarnane tulemus: anamnesis.dcase:226167)

Märkused: Isiku kaal 70kg eraldatakse teise kirjena.

VAJA: Mitte eraldada fotovarustuse kaalu? Raporteerida, et see kaal ei käi isiku enda kohta?


----

event_text:

siidid ex terve kõht verevalumites 
SpO2-99 ja p 80
RR oli haigla kõrge meil 150-90 
kaal -6 kg 111,2 kg nüüd 
metozok 50 edasi
twinsta 80-10 1 õ cardace asemel 

* value_category: 'KAAL'
* value: '6'
* value_unit: 'kg'
* value_ref_high: '111.2'
* epi_id: '47542469'

VAJA:

* Tuvastada kaalu langus?
* Tuvastada kaal kui 111.2? Seda kaalu praegu eraldi kirjena ei eraldata. 

----

event_text:

hematoomid taanduvad kaal -10 kg 
RR 125-80 p 90

* value_category: 'KAAL'
* value: '10'
* value_unit: 'kg'
* epi_id: '47542469'

VAJA: Tuvastada kui kaalu langus mitte kui isiku kaal?

----

event_text:

25.08.2014 - LOR: Kurk oli valus, praegu enam ei kaeba. Kuulmislangus aastaid. Obj: nina, neel, tonsillid, alaneel ja kõri puhtad, vähene hüpereemia. Vasakul vaikummistus. Loputus ja tualett. Kuulmekilede leid bilat. normis. Sk 0/0. Tk paremal 1-2m, vasakul 4m.
H91.1 Vanaduskuulmisnõrkus. H61.2 Vaikummistus vasakul.

* value_category: 'KAAL'
* value: '0'
* value_unit: ''
* epi_id: '32316017'

VAJA: Pole nagu kaal? Kas tasub eraldamist, kui ühikut ei õnnestu tuvastada, või väärtus on 0?

----

* event_text:

- Pt kaebab peavalu, valud kuklas, kaelas, suremistunne par käes. Pt-l 7-kuune poeg, kes kaalub 9 kg - raskuste tõstmine, sundasend.

* epi_id: 14132105
* value_category: 'KAAL'
* value: '9'
* value_unit: 'kg'

VAJA: Kommunikeerida, et kaal ei käi mitte isiku enda vaid lapse kohta?

----

Kategooriaga 'KAAL' seotud väikseid väärtusi ja nendega seotud tekste leiab filtri abil stiilis:

`... WHERE (value_category = 'KAAL') and (value::float < 10);`

----

Tabeli egcut_epi.work.microrun_201905301144_objective_finding_with_text põhjal:

* field_text:

...
Transperineaalne uuring: ureetra pikkus 2,9 cm, sisesuue on suletud, retrovesikaalnurk 80 kraadi. Valsalva katsul tüüpilised stressinkontinentsuse tunnused: ureetra roteerub sümfüüsi alla, retrovesikaalnurk suureneb 133 kraadini, ureetra sisesuue avaneb 5 mm, põiekaela destsensus 2,5 cm. 
...

* value_category: 'PIKKUS'
* value: '2.9'
* value_unit: 'cm'
* epi_id: '8047459'

VAJA: Pole patsendi pikkus. Kas peaks olema mingi eraldi väli, mis ütleks, et mille 
      kohta obj.leiu mõõtmine käib täpsemalt?

-----
