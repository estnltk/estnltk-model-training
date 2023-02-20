# Märgendusjuhend

## I. Lõppeesmärk 
Märgendamise lõppeesmärk on jagada laiendid lauses eemaldamise tulemuse järgi nelja kategooriasse: 

* seotud laiendid (`bound_entity`)
* eemaldatavad vabad laiendid (`free_entity`)
* sõnajärge muutvad vabad laiendid (`unnatural_sentences`) 
* valed laiendid (märgitud sõna pole laiend) (`incorrect_entity`)

Seotud laiendid on laiendid mille eemaldamine lausest muudab lause mõtet. 
Vabade laiendide eemadamine jätab lause mõtte samaks, kaovad vaid detailid näiteks kuidas või kus tegevus toimub. 
Enamasti on vabade ja seotud laiendite eristamine lihtne. On mõned erandid, mida me käsitleme põhjalikumalt allpool.

Näiteks on _väljavahetamist vajavate_ eemaldatav vaba laiend lauses

> " Süveneme Tartu liinibusside olukorda , et teada saada <u>**väljavahetamist vajavate**</u> busside hulk . 

sest selle eemaldamine ei muuda lause põhisisu ja alles jääb loomuliku sõnajärjega lause

> " Süveneme Tartu liinibusside olukorda , et teada saada busside hulk .

Samas võib vaba laiendi eemaldamine tuua kaasa vajaduse allesjäänud lause sõnajärge muuta. Näiteks on laiend _mullu_ lauses 

> <u>**Mullu**</u> kuulutas ajakiri People ta lahutatud lapsevanemate kategoorias musterisaks.

vaba laiend, kuid laiendi eemaldamisel tekib ebaloomuliku sõnajärjega lause  

> Kuulutas ajakiri People ta lahutatud lapsevanemate kategoorias musterisaks.

mille asemel tuleks kasutada ümberjärjestatud sõnajärjega lauset

> Ajakiri People kuulutas ta lahutatud lapsevanemate kategoorias musterisaks.

Meie töö kontekstis on oluline eristada vabasid laiendeid, mille eemaldamine sunnib muutma lause sõnajärge. 
Kuna sellised ebaloomuliku lausejärjega laused ei esine tavatekstides, siis selliste lausete kasutamine süntaksianalüsaatori treenimisel ja evalveerimisel pole otstarbekohane

**Teooria versus praktika**

Keeleteadus käsitleb vabade ja seotud laiendite teemat (M. Erelt, M. Metsalang. Eesti Keele Süntaks, 2017) palju täpsemat kui antud märgenduse saamiseks vaja on: 
> Seotud laiendid on tüüpjuhul süntaktiliselt obligatoorsed, vabad
laiendid fakultatiivsed. Siiski ei ole laiendi obligatoorsus või fakultatiivsus laiendi seotuse või vabaduse absoluutne näitaja. Komplement võib tihtipeale ära jääda verbist sõltumatutel põhjustel, näiteks indefiniitsena. Vrd Ta suitsetab sigaretti. – Ta suitsetab.
Teiselt poolt võib vaba laiend mõnikord olla obligatoorne. Nt
Ta käitub halvasti. – Ta käitub. Tal on paha tuju. – Tal on tuju.

Antud märgenduse loomise peaeesmärk on leida lühendatud laused, milles on lause süntaks sama kui originaalauses. Sealjuures on eristus vabadeks ja seotud laienditeks instrumentaalne. Absoluutne täpsus laiendite klassifitseerimisel pole oluline seni kuni lühendatud lause säilitab süntaksi. Loomulikult on korrektne kasulik, kuna võimaldab täpsemini uurida, millised seotud laiendid säilitavad süntaksi ja annab potentsiaalse võimaluse luua (reeglipõhiseid) automaatmärgendajaid. 


## II. Märgendusskeem

Praktilisel märgendamisel on otstarbekas vasta küsimustele

* Kas lühendatud lause on loomuliku sõnajärjega?
* Kas eemaldatav tekstifragment on laiend?
* Kas eemaldatav tekstifragment on vaba laiend?


## III. Iseloomulikud näited

### Eemaldatavad vabad laiendid

Laiend on vaba ja eemaldamisel jääb lause mõte piisavalt samaks ning ei teki ebaloomulikku sõnade järjekorda: 

> Enamus <u>**hõivatuist**</u> töötab alalisel töökohal . <br/>
> Enamus töötab alalisel töökohal .

> <u>**Rühti parandavas**</u> treeningus pole oluline mitte koormuse intensiivsus , vaid harjutuste õige sooritustehnika , sobivate koormuste leidmine ja suurem koormuste arv .<br/>
> Treeningus pole oluline mitte koormuse intensiivsus , vaid harjutuste õige sooritustehnika , sobivate koormuste leidmine ja suurem koormuste arv .


Kuigi lause sisu muutub jääb lause põhiolemus samaks:

> Ehk just selles peitub vastus küsimusele <u>**, miks on neist iga kaheksas hõivatud nii riigiettevõtete juhtkonnas kui riigiametites**</u> . <br/>
> Ehk just selles peitub vastus küsimusele .


### Seotud laiendid

Laiend on seotud ja eemaldamisel muutub lause arusaamatuks:

>  Leivamõhul oli huvitav kuju ja hea lõhn , oli tahtmine <u>**tema sisse lennata ning seal natuke nokkida**</u> . <br/>
> Leivamõhul oli huvitav kuju ja hea lõhn , oli tahtmine .

>  Leivamõhul oli <u>**huvitav**</u>  kuju ja hea lõhn , oli tahtmine tema sisse lennata ning seal natuke nokkida . <br/>
> Leivamõhul oli kuju ja hea lõhn , oli tahtmine tema sisse lennata ning seal natuke nokkida .

>Ma pole ammu lugenud <u>**nii vihkavat**</u> esseed kui see Kaplinski oma . <br/>
>Ma pole ammu lugenud esseed kui see Kaplinski oma .


### Sõnajärge muutvad vabad laiendid 

Kuigi laiend on vaba, siis eemaldamisel tekib ebaloomulik sõnade järjekord:

> Oluline siinkohal on see <u>**, et lepped olid konsensuslikud**</u> . <br/>
> Oluline siinkohal on see .



### Ebakorrektsed laiendid

Siia lähevad süntaksi vigadest tingitud laiendite moodustamise vead. 
Üheks kõige tüüpilisemaks on kogu lause sisu märkimine laiendiks, mistõttu lühendatud lause on tühi.



### Üleliigse komaga või kirjavahemärgiga laiend

Mõnikord on laiendi vaba, kuid selle eemaldamisel jääb koma või muu kirjavahemärk lauses valesse kohta:

><u>**Näiteks**</u> : olen söögitoa akna juures või magamistoa keskel . <br/>
>: olen söögitoa akna juures või magamistoa keskel .

>Ja see hale vahendatud maailm tundub meile millegipärast olulisena , <u>**rohkem eluna kui elu**</u> . <br/>
>Ja see hale vahendatud maailm tundub meile millegipärast olulisena , .

Need tuleks vastavalt märgendada kas "redundant comma" või "other redundant punctuation".


### Küsitav (dubious)

Vahel võib olla lause konteksti teadmata raske märgendada laiendit vabaks/seotuks. 

Lausete märgendamine küsitavaks ei ole eelistatud variant ning see on mõeldud erijuhtudeks.

