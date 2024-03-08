# morphological_tokenization

Hetkel töös (2024.03.08): WordpieceTokenizer'i jaoks sobiv leksikon, kus on morfi poolt väljaarvutatud
tükid (tüved, lõppud).

## 1. Korpuse pealt teeme sõnavormide sagedusloendi

Sagedusloendi esimeses reas on veergude pealkirjad.
Järgmised read on kujul

`sagedus` `tühik` `sõnavorm`

Read on järjestud sageduse järgi kahanevalt.

## 2. Sagedusloendis olevate sõnavormide tükeldaminie tüvedeks ja lõppudeks ja tekkivate tükkide arvu loendamine

* Lähtekoodi allalaadimine

```bash
mkdir ~/git ; cd git
git clone --depth 1 https://github.com/Filosoft/vabamorf.git vabamorf_github
```

* Kompileerime programmid

```bash
cd ~/git/vabamorf_github/apps/cmdline/project/unix
make -s -j all
make -s -j -f Makefile_split_tokens all
```

Kompileeritud programm on `~/git/vabamorf_github/apps/cmdline/project/unix/split_tokens`.
Programm vajab morf analüsaatori sõnastikku `~/git/vabamorf_github/dct/binary/et.dct`.

* Tükeldame sagedusloendis olevad sõnavormid tüvedeks ja lõppudeks

```bash
 ~/git/vabamorf_github/apps/cmdline/project/unix/split_tokens \
        --path ~/git/vabamorf_github/dct/binary/ \
        vormide-sag.txt > tykid.tsv
```

Faili `tykid.tsv` esimeses reas on veerunimed. Tageli veerud on eraldatud '\t' sõmboli abil.

Veergude sisu:

* **_1. veerg_** sõnavormi sagedus sisendfailist
* **_2. veerg_** sõnavorm sisendfailist
* **_3. veerg_** tüvedeks ja lõppudeks tükeldatid sõnavorm, näit `[ riigi ##kogu ]`, `[ kinnita ##s ]`
* **_4. veerg_** need jooksva sõnavormi tüved, mida varasemates sõnavormides ei esinenud
* **_5. veerg_** siiamaani leitud erinevate tüvede arv
* **_6. veerg_** need jooksva sõnavormi lõpud, mida varasemates sõnavormides ei esinenud
* **_7. veerg_** kokku erinevaid tüvesid ja lõppusid

Näiteks

```tsv
kokku	sõnavorm	tükeldus	uued_tüved	kokku_tüvesid	uued_lõpud	kokku_tüvesid+lõppusid
27214	ja	[ ja ]	[ ja ]	1	[ ]	1
19184	on	[ on ]	[ on ]	2	[ ]	2
13810	ei	[ ei ]	[ ei ]	3	[ ]	3
12314	et	[ et ]	[ et ]	4	[ ]	4
10170	ta	[ ta ]	[ ta ]	5	[ ]	5
8861	oli	[ ol ##i ]	[ ol ]	6	[ i ]	7
8599	kui	[ kui ]	[ kui ]	7	[ ]	8
6191	ka	[ ka ]	[ ka ]	8	[ ]	9
6114	see	[ see ]	[ see ]	9	[ ]	10
5329	oma	[ oma ]	[ oma ]	10	[ ]	11
5274	aga	[ aga ]	[ aga ]	11	[ ]	12
4454	ma	[ ma ]	[ ma ]	12	[ ]	13
4409	ning	[ ning ]	[ ning ]	13	[ ]	14
4391	mis	[ mis ]	[ mis ]	14	[ ]	15
4238	siis	[ siis ]	[ siis ]	15	[ ]	16
3523	nii	[ nii ]	[ nii ]	16	[ ]	17
3310	või	[ või ]	[ või ]	17	[ ]	18
3177	seda	[ se ##da ]	[ se ]	18	[ da ]	20
```

## Teeme etteantud suurusega tükikeste loendi

Võtame sagedusloendi sagedasemate sõnavormide tüvede ja lõppude hulgast
etteantud koguse tüvesid ja lõppe.

```bash
./create_lexicon.py --max=10000  --indent=4 tykid.tsv  > lexicon.txt
```

Katkend failist `lexicon.txt`

```text
ja
on
ei
et
ta
ol
##i
kui
ka
see
oma
aga
ma
ning
mis
siis
nii
või
se
##da
nagu
kes
tema
vee
##l
pole
ku
##id
sell
##e
kas
juba
na
##d
välja
mi
##dagi
kõik
```

## Teeme etteantud suurusega leksikoni: `sõnavorm` : `tükeldus`

```bash
./create_dct.py --max=20000 --indent=4 tykid.tsv
```

Võtab sageustabeli tipust niimitu sõnavormi, et kokku tuleks etteantud
arv (antud näites 20000) unikaalset tüve ja lõppu ja teeb neist JSON sõnastiku
kus sõnavormile vastab tema tükeldus tüvedeks ja lõppudeks.

Näiteks midagi sellist:

```json
{
    "ja": "ja",
    "on": "on",
    "ei": "ei",
    "et": "et",
    "ta": "ta",
    "oli": "ol ##i",
    "kui": "kui",
    "ka": "ka",
    "see": "see",
    "oma": "oma",
    "aga": "aga",
    "ma": "ma",
    "ning": "ning",
    "mis": "mis",
    "siis": "siis",
    "nii": "nii",
    "või": "või",
    "seda": "se ##da",
    "nagu": "nagu",
    "kes": "kes",
    "tema": "tema",
    "veel": "vee ##l",
    "pole": "pole",
    "kuid": "ku ##id",
    "selle": "sell ##e",
    "kas": "kas",
    "juba": "juba",
    "nad": "na ##d",
    "välja": "välja",
    "midagi": "mi ##dagi",
}
```
