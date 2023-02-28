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

> Veebruarikuus on Marbella kant ilmselt üks väheseid kohti Euroopas , kus <u>**ilmaüllatusi kartmata**</u> selliseid üritusi korraldada võib . <br/>
> Veebruarikuus on Marbella kant ilmselt üks väheseid kohti Euroopas , kus selliseid üritusi korraldada võib .


Kuigi lause sisu muutub jääb lause põhiolemus samaks:

> Alati  <u>**, kui ma olen söönud ,**</u> hakkab mul kõht valutama ! " <br/>
> Alati hakkab mul kõht valutama ! "

> " Teatud mõttes oleme selleks valmis <u>**, sest meil ei ole ühtegi oma apteeki**</u> , " ütleb Vaasa . <br/>
> " Teatud mõttes oleme selleks valmis , " ütleb Vaasa .


### Seotud laiendid

Laiend _ei_ on seotud ja eemaldamisel muutub lause arusaamatuks:

> Kui sama trend jätkub , on aasta lõpuks üle 80 inimese saanud rohkem kui <u>**miljonikroonise**</u> auto omanikuks . <br/>
> Kui sama trend jätkub , on aasta lõpuks üle 80 inimese saanud rohkem kui auto omanikuks .

> " Tülpinud ? " rohkem nentis <u>**kui küsis**</u> kõrvalistuja . <br/>
> " Tülpinud ? " rohkem nentis kõrvalistuja .


### Sõnajärge muutvad vabad laiendid 

Kuigi laiend on vaba, siis eemaldamisel tekib ebaloomulik sõnade järjekord:

> <u>**Liinati arvates**</u> võis neid olla isegi kuni 10 000 . <br/>
> võis neid olla isegi kuni 10 000 .

> <u>**Tema tagasi tulles**</u> istus Aune ikka oma tugitoolis , hommikumantel kurguni kinni .<br>
> Istus Aune ikka oma tugitoolis , hommikumantel kurguni kinni .



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

