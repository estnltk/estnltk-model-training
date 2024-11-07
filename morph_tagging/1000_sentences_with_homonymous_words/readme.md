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

### Andmeformaat

Andmed on [jl formaadis](https://jsonlines.org/), igas json kirjes on toodud lause tekst (`"text"`), homonüümne sõna (`"word"`), (automaatselt määratud) sõnaliik (`"partofspeech"`) ning sõna täpne asukoht lauses (vastavad `"start"`, `"end"` indeksid). Täpne asukoht on toodud seetõttu, et sõna võib esineda lauses ka mitu korda. Lisaks on toodud lause metaandmed: millisest ühendkorpuse failist lause pärines (`"corpus"`), vastava dokumendi id korpuses (`"doc_id"`) ning vastava lause number (`"sent_id"` - siin ei mõelda mitte lause järjekorranumbrit dokumendis, vaid lause järjekorranumbrit kõigi vormihomonüümiaga sõnu sisaldavate lausete hulgas).
 
Näidiskirje (muuttüüp 17): `{"corpus": "nc19_Reference_Corpus.vert", "doc_id": 299912, "sent_id": 2110560, "text": "See oli raske elu.", "word": "elu", "partofspeech": "S", "start": 14, "end": 17} `

### Labelstudio kujule teisendamine

Skript [`export_to_labelstudio.py`](export_to_labelstudio.py) teisendab jl failid [Labelstudio](https://labelstud.io/) JSON failideks ning tekitab vastavad märgenduseliidese (_labeling interface_) kirjeldused. 
JSON failid ja liidest kirjeldavad tekstifailid kirjutatakse kausta [to_labelstudio](to_labelstudio/).