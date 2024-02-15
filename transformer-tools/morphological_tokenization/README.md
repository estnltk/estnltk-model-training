# Funktsioon punktuatsiooni sõnast lahkutõstmiseks ja sõna jagamiseks tüve, liitsõna, lõpu jne piirilt tükikesteks

## Eeldused

Kokkukompileeritud programm `split_tokens` ja morf analüsaatori leksikon `et.dct`.
Ubuntu 22.04 LTS jaoks kokkukompileerutd programm on repos olemas.
Ise kompileerimiseks järgi [juhiseid](https://github.com/Filosoft/vabamorf/blob/master/apps/cmdline/split_tokens/README.md)

## Kasutusnäide

```bash
python3 split_tokens.py test.txt 
pee ti gi " tere " talv ! “ tere ” mee s vana isa dele pro aktiivse tele apoliitiline õnne likkus
```