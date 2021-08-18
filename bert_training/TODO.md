# TODO

Kuskil on Sveni algne spec, aga seda ei ole siin repos.

Järgnevas on toodud mõningad tähelepanekud mis ei ole niivõrd speciga seoses, 
vaid lihtsalt mõned ebakohad mis võiks leida tähelepanu ja parendamist.

* [ ] Step01.ipynb ja Step02.ipynb seest viidatud joonised (.png failid) ei ole Gitlabi  
  keskkonnas (nt https://gitlab.cs.ut.ee/health-informatics/medbert/-/blob/master/Step02.ipynb)
  ega nt IntelliJ IDEA keskkonnas nähtavad, ei renderdu (katkise pildi ikoon).
* [ ] Jooniste algkoodi/alg-faili pole (pole võimalik joonist kohendada)
  * Variant 1: Lisada algfail, mida saab muuta
  * Variant 2: Teha joonis millegagi, mille algkoodi/alg-faili saab muutmiseks avada ja 
    PNG maha salvestada; nt [yEd](https://www.yworks.com/products/yed)
  * Variant 3: Teha joons ASCII tekstina, nt https://asciiflow.com/
               (või selle vanema variandi https://asciiflow.com/legacy/) vmt abilise abil
* Testid ja näited:
  * [x] Kas 1000-realisest failist (nt `"\\data\\egcut_epi_mperli_texts_1000.tsv"`) 
    mis ei saa repos olla (sest me ei taha siin päris andmeid hoida)
    saaks teha repos hoitava 10-realise faili, mis on formaadilt sama 
    (ja seega oleks hästi täpselt näha, et mis kujul lubatud/oodatud sisendformaadid on) 
    aga sisult piisavalt obfuskeeritud 
    (üle digiloo otsides selliseid fraase ei leiaks, inimesi kuidagi ei identifitseeriks)?
* Setup: 
  * [ ] Kirjeldada algne env setup/keskkonna setup (Pythoni ja teekide muretu install), sh
        et mida saab teha/testida lokaalselt, ja mis vajab GPUsid / UTHPC keskkonda; vt ka [Step00.md](Step00.md)
  * [ ] Kirjeldada käitamine UTHPC keskkonnas, abistavad skriptid selleks
