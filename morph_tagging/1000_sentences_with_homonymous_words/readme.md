## Vormihomonüümia andmestik (muuttüübid 1, 16, 17, 19)

Andmestik sisaldab juhuvalikut Eesti Keele Ühendkorpuse Koondkorpuse lausetest (täpsemalt: .vert failidest `nc19_Balanced_Corpus.vert` ja `nc19_Reference_Corpus.vert`), mis sisaldavad järgmistesse muuttüüpidesse kuuluvaid vormihomonüümiaga sõnu:

* muuttüüp 1: 
	* sõnad, mis on mitmesed nimetava ja omastava vahel: `sg n`, `sg g`
	* näiteks: _akadeemia_ (nimetav), _akadeemia_ (omastav)

* muuttüüp 16:
	* sõnad, mis on mitmesed nimetava ja omastava vahel: `sg n`, `sg g`
	* näiteks: _kõne_ (nimetav), _kõne_ (omastav)

* muuttüüp 17:
	* sõnad, mis on mitmesed nimetava, omastava ja osastava vahel: `sg n`, `sg g`, `sg p` 
    * näiteks: _muna_ (nimetav), _muna_ (omastav), _muna_ (osastav)
        
* muuttüüp 19:
	* sõnad, mis on mitmesed omastava, osastava ja lühikese sisseütleva vahel: `sg g`, `sg p`, `adt`
    * näiteks: _admirali_ (omastav), _admirali_ (osastav), _admirali_ (lühike sisseütlev)

Iga muuttüübi juhuvalim sisaldab 1000 lauset.

### Andmestiku ettevalmistamise skriptid:

* `01_find_homonymous_forms_fs_lex.py` -- leiab muuttüüpidesse 1, 16, 17, 19 kuuluvad sõnad Vabamorfi leksikoni põhjal (kasutame Sander Saska [lõputöö eksperimentidest](https://github.com/SanderSaska/MorfoloogiliseMuuttyybiAutomaatneTuvastaja) faili [`andmed_algvormidega.csv`](https://github.com/SanderSaska/MorfoloogiliseMuuttyybiAutomaatneTuvastaja/blob/main/andmed_algvormidega.csv), mis on tuletatud Vabamorfi leksikonist `fs_lex`), genereerib selliste sõnade kõik vormid EstNLTK süntesaatori abil ning salvestab homonüümsed (sama kirjapildiga) vormid faili `homonymous_forms_1_16_17_19.csv`;

* `02_extract_homonymous_forms_from_koondkorpus.py` -- eraldab homonüümseid sõnavorme (sisendfail `homonymous_forms_1_16_17_19.csv`) sisaldavad laused morfoloogiliselt märgendatud Tasakaalus korpuse ja Koondkorpuse .vert failidest (`nc19_Balanced_Corpus.vert` ja `nc19_Reference_Corpus.vert`). Loob 2 tulemusfaili: 
	* `all_enc_koond_homonymous_forms_counts.jl` -- fail, kus on homonüümsete vormide sagedused koos võimalike puuduvate vormide loendiga. Igal faili real on ühe vormi statistika json objektina, näiteks: `{"word": "kilu", "total": 437, "corpus_forms": [["S_sg n", 415], ["S_sg g", 20], ["S_sg p", 2]], "missing_forms": []}`
	  * kokku ca 5500 unikaalset vormi;
	  * sh ~30% homonüümsetest sõnadest pärisnimeanalüüsiga;
	  * sh ~70% homonüümsetest sõnadest pärisnimeanalüüsita;

	* `all_enc_koond_homonymous_forms_sentences.jl` -- fail, kus on homonüümseid vorme sisaldavad laused. Igal faili real on üks lause json objektina, näiteks: `{"corpus": "nc19_Balanced_Corpus.vert", "doc_id": 13, "text": "Püügilimiit, mille piires võivad Eesti kalurid käesoleval aastal püüda on kokku 41 200 t kilu ja 39 000 t räime.", "forms": [{"word": "Eesti", "form": "H_sg g", "start": 33, "end": 38}, {"word": "kilu", "form": "S_sg n", "start": 89, "end": 93}]}`
	  * kokku ca 4.8 milj lauset;
	  * sh ~52% lausetest sisaldab pärisnimeanalüüsiga homonüümset vormi;
	  * sh ~48% lausetest ei sisalda ühtegi pärisnimeanalüüsiga homonüümset vormi;

* `03_pick_1000_sentences_from_each_infl_type.py` -- teeb juhuvaliku kõigi sihtmuuttüüpide (1, 16, 17, 19) sõnu sisaldavate lausete seas, valides iga muuttüübi kohta 1000 juhuslikku lauset (ilma kordusteta, sealjuures võtab laused, kus on vähemalt 4 sõna, vähemalt üks väiketähega sõna, aga mitte rohkem kui 1500 sümbolit). Salvestab tulemused kausta [random\_pick\_jl](random_pick_jl/), failidesse nimega `f'infl_type_{infl_type}_randomly_picked_1000_sentences_pick_{run_id}.jl'`, kus `{infl_type}` on sihtmuuttüübi number ja `{run_id}` on skripti jooksutamise järjekorranumber. Teeb väljundfailide põhjal kindlaks, kui palju on skripti varem jooksutatud ning vaikimisi välistab varasemalt valitud lausete sattumise uue jooksutamise juhuvalimisse. Ühtlasi väljastab statistikat muuttüüpide kaupa.

  	*Väljundi formaat*. Andmed on [jl formaadis](https://jsonlines.org/), igas json kirjes on toodud lause tekst (`"text"`), homonüümne sõna (`"word"`), (automaatselt määratud) sõnaliik (`"partofspeech"`) ning sõna täpne asukoht lauses (vastavad `"start"`, `"end"` indeksid). Täpne asukoht on toodud seetõttu, et sõna võib esineda lauses ka mitu korda. Lisaks on toodud lause metaandmed: millisest ühendkorpuse failist lause pärines (`"corpus"`), vastava dokumendi id korpuses (`"doc_id"`) ning vastava lause number (`"sent_id"` - siin ei mõelda mitte lause järjekorranumbrit dokumendis, vaid lause järjekorranumbrit kõigi vormihomonüümiaga sõnu sisaldavate lausete hulgas).
 
    Näidiskirje (muuttüüp 17): `{"corpus": "nc19_Reference_Corpus.vert", "doc_id": 299912, "sent_id": 2110560, "text": "See oli raske elu.", "word": "elu", "partofspeech": "S", "start": 14, "end": 17} `

* `04_export_to_labelstudio.py` -- teisendab jl failid [Labelstudio](https://labelstud.io/) JSON failideks ning tekitab vastavad märgenduseliidese (_labeling interface_) kirjeldused. Teisendamise funktsionaalsuse kasutamiseks on vajalik EstNLTK versioon 1.7.4+. JSON failid ja liidest kirjeldavad tekstifailid kirjutatakse kausta [to\_labelstudio](to_labelstudio/) alamakaustadesse, alamkausta number vastab `{run_id}`-ile sisendfaili nimes, nt:

	* `infl_type_01_randomly_picked_1000_sentences_pick_1.jl` sisu läheb kausta `to_labelstudio/1`
	* `infl_type_01_randomly_picked_1000_sentences_pick_2.jl` sisu läheb kausta `to_labelstudio/2`
	* jne



