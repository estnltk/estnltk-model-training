# morphological_tokenization

## 0. Eeltöö

Repo kloonimine

```cmdline
mkdir -p ~/git ; cd git
git clone git@github.com:estnltk/estnltk-model-training.git estnltk_model_training_github 
```

Virtuaalkeskkonna loomine:

```cmdline
export workspaceFolder=~/git/estnltk_model_training_github
cd ${workspaceFolder}/transformer-tools/morphological_tokenization/bin/
./create_venv.sh
```

## 1. Tekstikorpusest html-märgendite kustutamine

Labane lahendus: kustutame kõik mis jääb `<` ja `>` vahele.

```cmdline
cd ${workspaceFolder}/transformer-tools/morphological_tokenization/bin

venv/bin/python3 remove_html.py \
    --in_html=../training/dataset_training_bigger/ettenten_0.fshtml \
    --out_txt=../training/dataset_training_bigger/ettenten_0.txt
```

## 2. Korpuse pealt sõnavormide sagedusloendi tegemine

Tõstame sõna külge kleepunud punktuatsiooni jms lahku.

Sagedusloendi read on järjestud sageduse järgi kahanevalt ja kujul

`sagedus` `tühik` `sõnavorm`

Sagedusloendi tegemise näide:

```cmdline
venv/bin/python3 tee_sonede_sagedusloend.py \
    --files=../training/dataset_training_bigger/ettenten_0.fshtml \
    --out=../training/dataset_training_bigger/ettenten_0.freq
```

Read on järjestud sageduse järgi kahanevalt.

## 3. Sagedusloendis olevate sõnavormide tükeldaminie tüvedeks ja lõppudeks ja tekkivate tükkide arvu loendamine

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

Mugavuse huvides olen need kopeerinud kataloogi `~/git/huggingface_github/estnltk_model_training_github/transformer-tools/morphological_tokenization`.

* Tükeldame sagedusloendis olevad sõnavormid tüvedeks ja lõppudeks

```bash
~/git/vabamorf_github/apps/cmdline/project/unix/split_tokens \
        --path ~/git/vabamorf_github/dct/binary/ \
        vormide-sag.txt > ettenten_0_tykid.tsv
```

või

```bash
./split_tokens ../training/dataset_training_bigger/ettenten_0.freq \
> ../training/dataset_training_bigger/ettenten_0_tykid.tsv
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

## 4. Etteantud suurusega tükikeste loendi tegemine

Võtame sagedusloendi sagedasemate sõnavormide tüvede ja lõppude hulgast
etteantud koguse (10000 tk) tüvesid ja lõppe.

```bash
venv/bin/python3 create_lexicon.py --max=10000  \
    ../training/dataset_training_bigger/ettenten_0_tykid.tsv \
> ../training/dataset_training_bigger/lexicon_10000.txt
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

## Treenime: leksikoni tegemine ainult algse teksti pealt

Leksikoni suuruseks võtame 100000

```bash
venv/bin/python3 train_bert_wordpiece.py \
    --size=100000 \
    --files=../training/dataset_training_bigger/ettenten_0.txt \
    --out=../training/dataset_training_bigger/vocab4bert/ --name=text_bert_wordpiece
```

## Treenime: leksikoni tegemine etteantud leksikoni ja algse teksti pealt

Väljundleksikoni suurus 100000, miksime kokku eespool tehtud 10000 sõna leksikoniga.

```bash
venv/bin/python3 train_bert_wordpiece.py \
    --size=100000 \
    --vocab=../training/dataset_training_bigger/lexicon_10000.txt \
    --files=../training/dataset_training_bigger/ettenten_0.txt \
    --out=../training/dataset_training_bigger/vocab4bert/ --name=text+lex_bert_wordpiece
```

## Järjestame mõlemad leksikonide ja vaatame erinevusi

```bash
sort < ../training/dataset_training_bigger/vocab4bert/text_bert_wordpiece-vocab.txt \
     > ../training/dataset_training_bigger/vocab4bert/text_bert_wordpiece-vocab.txt.sort

sort < ../training/dataset_training_bigger/vocab4bert/text+lex_bert_wordpiece-vocab.txt \
     > ../training/dataset_training_bigger/vocab4bert/text+lex_bert_wordpiece-vocab.txt.sort


code --diff \
    ../training/dataset_training_bigger/vocab4bert/text_bert_wordpiece-vocab.txt.sort \
    ../training/dataset_training_bigger/vocab4bert/text+lex_bert_wordpiece-vocab.txt.sort
```

## Laseme teksti mõlema leksikoniga läbi

```bash
venv/bin/python3 tokenize.py \
    --vocab=../training/dataset_training_bigger/vocab4bert/text_bert_wordpiece-vocab.txt \
    --input=../training/dataset_training_bigger/ettenten_0.txt \
    --output=../training/dataset_training_bigger/ettenten_0_vocab_text.txt

venv/bin/python3 tokenize.py \
    --vocab=../training/dataset_training_bigger/vocab4bert/text+lex_bert_wordpiece-vocab.txt \
    --input=../training/dataset_training_bigger/ettenten_0.txt \
    --output=../training/dataset_training_bigger/ettenten_0_vocab_text+lex.txt 

code --diff \
    ../training/dataset_training_bigger/ettenten_0_vocab_text.txt \
    ../training/dataset_training_bigger/ettenten_0_vocab_text+lex.txt
```

## Teeme etteantud suurusega leksikoni: `sõnavorm` : `tükeldus`

Lihtsalt niisama, äkki pakub huvi

```bash
venv/bin/python3 create_dct.py --max=20000 --indent=4 tykid.tsv
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
