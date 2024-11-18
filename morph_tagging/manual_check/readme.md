## Morfoloogiliste analüüside erinevuste käsitsi hindamine ja märgendamine

**Lähteandmed:** Failis [`03_morph_analysis_vs_bert_morph_2_x1000_even.txt`](03_morph_analysis_vs_bert_morph_2_x1000_even.txt) on 1000 erinevust Berti morfoloogilise märgendaja ning Vabamorfi vahel. Mõlemad automaatmärgendajad kasutavad **Vabamorfi märgendeid** (kirjeldatud [siin](https://github.com/estnltk/estnltk/blob/main/tutorials/nlp_pipeline/B_morphology/00_tables_of_morphological_categories.ipynb)), mis on esitatud listina `[sõnaliik, vormitunnus]`. 

Iga erinevuse puhul on toodud välja: 1) viide, millisest alamkorpusest ja json failist erinevus pärineb, 2) erinevuse lausekontekst, kus on loogeliste sulgude abil märgitud erinev sõna, 3) automaatmärgendajate poolt väljapakutud tõlgendused. 

Näide:

<pre>
blogs_and_forums::web13_246643_x.json::(385, 387)::7523

...a. Olen seda teinud oma W208 CLK ja võin kinnitada, juhised  {on}  õiged\n\nJärgige neid juhiseid omal vastutusel , see töötas m...
--- COMMON   --------------------------------------------------
morph_analysis_flat        ['V', 'b']
bert_morph_tagging_flat    ['V', 'b']
--- MISSING  --------------------------------------------------
morph_analysis_flat        ['V', 'vad']
</pre>

**Ülesanne:** Ülesandeks on tekstifailis `03_morph_analysis_vs_bert_morph_2_x1000_even.txt` märkida korrektse analüüsirea järele `(+)`. Eelmise näite puhul:

<pre>
blogs_and_forums::web13_246643_x.json::(385, 387)::7523

...a. Olen seda teinud oma W208 CLK ja võin kinnitada, juhised  {on}  õiged\n\nJärgige neid juhiseid omal vastutusel , see töötas m...
--- COMMON   --------------------------------------------------
morph_analysis_flat        ['V', 'b']
bert_morph_tagging_flat    ['V', 'b']
--- MISSING  --------------------------------------------------
morph_analysis_flat        ['V', 'vad']  (+)
</pre>

Soovitatav on faili redigeerida [Notepad++]( https://notepad-plus-plus.org/)'i abil.

* Kui on mõlemad süsteemid pakkusid korrektse analüüsi, siis märgendada mõlemad. Näiteks:

<pre>
blogs_and_forums::web13_290189_x.json::(1104, 1106)::71188

.../ehk ei jookse külmkapil kompressor veel lühisesse\n\nVaimsus  {on}  pläma ja pläma on vaimsus.\n\nsun goes down?, 2005-11-02 17:3...
--- COMMON   --------------------------------------------------
morph_analysis_flat        ['V', 'b']  (+)
bert_morph_tagging_flat    ['V', 'b']  (+)
--- MISSING  --------------------------------------------------
morph_analysis_flat        ['V', 'vad']
</pre>

* Kui on mõlemad süsteemid eksisid, siis tekitada uus rida nimega `manual` ja lisada õige analüüs koos lõppeva `(+)` märgiga. Näide:
<pre>
wikipedia::wiki17_178872_x.json::(9758, 9759)::358556

...rmistuse!) kaheldes teaduse reeglite süsteemis -teaduses on  {~} selged reeglid: meetodid, metodoloogia, jms Ats 08:16, 13 ma...
--- MISSING  --------------------------------------------------
morph_analysis_flat        ['Y', '?']
--- EXTRA    --------------------------------------------------
bert_morph_tagging_flat    ['A', 'pl n']
manual                     ['Z', '']  (+)
</pre>

* Kui pole kindel, kas käsitsi valitud analüüs on õige, siis võib `(+)` asemel kasutada `(?)`.


## Tulemused

Faili [`03_morph_analysis_vs_bert_morph_2_x1000_even_checked.txt`](03_morph_analysis_vs_bert_morph_2_x1000_even_checked.txt) käsitsi hindamise tulemused:

<pre>
 both correct, but vabamorf ambiguous               297/1000  29.70%
 only bert_morph_tagging_flat correct               289/1000  28.90%
 only morph_analysis_flat correct and unambiguous   202/1000  20.20%
 only manual correct                                75/1000  7.50%
 only morph_analysis_flat correct, but ambiguous    68/1000  6.80%
 difficult to tell                                  10/1000  1.00%
</pre>