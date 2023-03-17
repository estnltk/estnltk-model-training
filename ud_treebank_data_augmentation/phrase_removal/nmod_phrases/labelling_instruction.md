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

Näiteks on _piisavalt_ eemaldatav vaba laiend lauses

> Tähtis on omada <u>**piisavalt**</u> raha, et oleks mugav olla.

sest selle eemaldamine ei muuda lause põhisisu ja alles jääb loomuliku sõnajärjega lause

> Tähtis on omada raha, et oleks mugav olla.

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

> 1999. aastal saatis Saksamaa esmakordselt pärast Teist maailmasõda <u>**oma**</u> sõdurid piiri taha Kosovosse rahu tagama . <br/>
> 1999. aastal saatis Saksamaa esmakordselt pärast Teist maailmasõda sõdurid piiri taha Kosovosse rahu tagama .


Kuigi lause sisu muutub jääb lause põhiolemus samaks:

>  <u>**Demokraatia**</u> üks nurgakivi , võimaluste võrdsus , mis küll juriidiliselt võib funktsioneerida , jääb kahjuks praktikas teostamatuks . <br/>
> Üks nurgakivi , võimaluste võrdsus , mis küll juriidiliselt võib funktsioneerida , jääb kahjuks praktikas teostamatuks .

> Kirjanike hulgas on olnud küll joodikuid , kuid tõsine , pea teaduslik tegelemine <u>**viinamarjadest saadud jookidega , mille austajaid viis aastat tagasi oli tunduvalt vähem kui praegu**</u> , oli ikkagi üllatav . <br/>
> Kirjanike hulgas on olnud küll joodikuid , kuid tõsine , pea teaduslik tegelemine , oli ikkagi üllatav . 


### Seotud laiendid

Laiend on seotud ja eemaldamisel muutub lause arusaamatuks:

> <u>**Kolmikhüppe maailmarekordi**</u> omanik Jonathan Edwards harjutas Sierra Nevada mägedes rulluiskudel , pidades seda tõsiseks valmistumisviisiks , mitte lõbuks . <br/>
> Omanik Jonathan Edwards harjutas Sierra Nevada mägedes rulluiskudel , pidades seda tõsiseks valmistumisviisiks , mitte lõbuks . 

> Tegemist on <u>**raamatutrükkimise**</u> algusega Eestimaal .<br/>
> Tegemist on algusega Eestimaal .


### Sõnajärge muutvad vabad laiendid 

Lause, milles on laiend vaba, aga selle eemaldamisel tekib lauses ebaloomulik sõnade järjekord.


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

