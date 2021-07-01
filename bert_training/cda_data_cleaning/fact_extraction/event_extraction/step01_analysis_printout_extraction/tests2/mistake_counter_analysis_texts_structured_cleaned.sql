DROP TABLE IF EXISTS tests_initial;
CREATE TEMP TABLE tests_initial AS

(
SELECT

    'count distinct values' as type,

---------------------------------------------------------------------------
         -- kogu ridade arv
---------------------------------------------------------------------------

       cast((select count(*) from work.run_202104231122_analysis_texts_structured_cleaned) as numeric)
                                                            as all_entries,
---------------------------------------------------------------------------
        -- analysis ja parameter name ei tohi korraga nullid või tühjad olla, checkida seda
---------------------------------------------------------------------------
       (select count(distinct analysis_name) from work.run_202104231122_analysis_texts_structured_cleaned
          WHERE (analysis_name is null AND parameter_name is null)
                    OR (analysis_name ~ '(^\s*$)' AND parameter_name is null)
                    OR (analysis_name is null AND parameter_name ~ '(^\s*$)')
                    OR (analysis_name ~ '(^\s*$)' AND parameter_name ~ '(^\s*$)')) as analysis_parameter_both_null,

---------------------------------------------------------------------------
       -- analysis name
---------------------------------------------------------------------------
        -- 'anonym' hulk analysis name hulgas
       (select count(distinct analysis_name)
       from work.run_202104231122_analysis_texts_structured_cleaned
           WHERE analysis_name LIKE '%anonym%'
              or analysis_name LIKE '%ANONYM%') as analysis_name_anonym,
       -- whitespace
       (SELECT count(distinct analysis_name) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE analysis_name ~ '(^\s*$)') as analysis_name_only_whitespace,
        -- digit
       (SELECT count(distinct analysis_name) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE analysis_name ~ '(^\d*$)'
           and analysis_name !~ '(^\s*$)') as analysis_name_only_digits,
       --
       (SELECT count(distinct analysis_name) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE analysis_name ~ '(\d)'
           and analysis_name !~ '(^\d*$)'
          and analysis_name !~ '([[:punct:]])' -- only looking entries with no punctuation
          and analysis_name not like 'Hemogramm 5-Osalise Leukogrammiga') as analysis_name_some_digits,

       -- punctuation
        (SELECT count(distinct analysis_name) FROM work.run_202104231122_analysis_texts_structured_cleaned
       WHERE analysis_name ~ '(^\W+$)') as analysis_name_only_punctuation,

       -- tavaliselt kui sisalduvad mingid kirjavahemärgid analysis names on asi veits kahtlane
       -- kuigi mõnikord sidekriips - on normaalne, siis pigem peab siin eraldi ignoreerima keisse kus ta on normaalne
       -- sest ta on sageli ka situatsioonides kus on eraldus jama

       -- Vb peaks some_punctuation splittima kaheks - some punctuation võtta - sisaldused ära ja teha
       -- eraldi tulp et palju on - sees kuskil


        (SELECT count(distinct analysis_name) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE analysis_name ~ '([[:punct:]])'
            and analysis_name !~ '(^[[:punct:]]$)'
            and analysis_name !~ '(\d)' -- only punctuation no numbers
            and analysis_name not like 'Hemogramm 5-Osalise Leukogrammiga') as analysis_name_some_punctuation,


       (SELECT count(distinct analysis_name) FROM work.run_202104231122_analysis_texts_structured_cleaned
       WHERE analysis_name ~ '([[:punct:]])'
          and analysis_name ~ '(\d)'
         and analysis_name !~'(\d\d:\d\d)' -- excluding time
         and analysis_name !~ '(\d\d\.\d\d\.\d\d)' -- excluding date
            and analysis_name not like 'Hemogramm 5-Osalise Leukogrammiga') as analysis_name_punctuation_and_digits,


    -- contains timestamp
       (SELECT count(distinct analysis_name) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE analysis_name ~'(\d\d:\d\d)'
                and analysis_name !~ '(\d\d\.\d\d\.\d\d)' -- no date here
           ) as analysis_name_contains_time,

       (SELECT count(distinct analysis_name) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE analysis_name ~ '(\d\d\.\d\d\.\d\d)'
              and analysis_name !~'(\d\d:\d\d)' -- no time here
           ) as analysis_name_contains_date,

       (SELECT count(distinct analysis_name) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE analysis_name ~ '(^\D$)'
          and analysis_name !~ '([[:punct:]])' ) as analysis_name_only_one_letter,

---------------------------------------------------------------------------
       -- parameter_name
---------------------------------------------------------------------------

       (select count(distinct parameter_name)
       from work.run_202104231122_analysis_texts_structured_cleaned
           WHERE parameter_name LIKE '%anonym%'
              or parameter_name LIKE '%ANONYM%'
           ) as parameter_name_anonym,
       -- whitespace
        (SELECT count(distinct parameter_name) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE parameter_name ~ '(^\s*$)'
            ) as parameter_name_only_whitespace,
        -- digit
        (SELECT count(distinct parameter_name) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE parameter_name ~ '(^\d*$)'
            and parameter_name !~ '(^\s*$)'
            ) as parameter_name_only_digits,

       (SELECT count(distinct parameter_name) FROM work.run_202104231122_analysis_texts_structured_cleaned
            WHERE parameter_name ~ '(\d)'
            and parameter_name !~ '(^\d*$)'
            and parameter_name !~ '([[:punct:]])' -- only digits no punctuation
           ) as parameter_name_some_digits,

       -- punctuation
        (SELECT count(distinct parameter_name) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE parameter_name ~ '(^\W+$)'
            ) as parameter_name_only_punctuation,

        (SELECT count(distinct parameter_name) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE parameter_name ~ '([[:punct:]])'
            and parameter_name !~ '(^[[:punct:]]$)'
            and parameter_name !~ '(\d)' -- only punctuation no numbers
         ) as parameter_name_some_punctuation,

       (SELECT count(distinct parameter_name) FROM work.run_202104231122_analysis_texts_structured_cleaned
       WHERE parameter_name ~ '([[:punct:]])'
          and parameter_name ~ '(\d)'
         and parameter_name !~'(\d\d:\d\d)' -- excluding time
         and parameter_name !~ '(\d\d\.\d\d\.\d\d)' -- excluding date
           ) as parameter_name_punctuation_and_digits,


       -- contains timestamp
       (SELECT count(distinct parameter_name) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE parameter_name ~'(\d\d:\d\d)'
                and parameter_name !~ '(\d\d\.\d\d\.\d\d)' -- no date here
           ) as parameter_name_contains_time,

       (SELECT count(distinct parameter_name) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE parameter_name ~ '(\d\d\.\d\d\.\d\d)'
              and parameter_name !~'(\d\d:\d\d)' -- no time here
           ) as parameter_name_contains_date,


       (SELECT count(distinct parameter_name) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE parameter_name ~ '(^\D$)'
          and parameter_name !~ '([[:punct:]])' ) as parameter_name_only_one_letter,


---------------------------------------------------------------------------
        -- parameter_unit
---------------------------------------------------------------------------
        --whitespace

        (SELECT count(distinct parameter_unit) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE parameter_unit ~ '(^\s*$)') as parameter_unit_only_whitespace,

              -- contains timestamp
       (SELECT count(distinct parameter_unit) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE parameter_unit ~'(\d\d:\d\d)'
                and parameter_unit !~ '(\d\d\.\d\d\.\d\d)' -- no date here
           ) as parameter_unit_contains_time,

       (SELECT count(distinct parameter_unit) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE parameter_unit ~ '(\d\d\.\d\d\.\d\d)'
              and parameter_unit !~'(\d\d:\d\d)' -- no time here
           ) as parameter_unit_contains_date,


       (SELECT count(distinct parameter_unit) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE  parameter_unit ~ '(^\d*$)'
            and parameter_unit !~ '(^\s*$)'
           ) as parameter_unit_only_digits,

        (SELECT count(distinct parameter_unit) FROM work.run_202104231122_analysis_texts_structured_cleaned
            WHERE parameter_unit ~ '(\d)'
            and parameter_unit !~ '(^\d*$)'
            and parameter_unit !~ '([[:punct:]])' -- only digits no punctuation
           ) as parameter_unit_some_digits,

        (SELECT count(distinct parameter_unit) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE parameter_unit ~ '(^\W+$)'  -- puctuation with some whitespace
          and parameter_unit !~ '(^%$)'  -- % is allowed
          and parameter_unit !~ '(^\s*$)' -- only whitespace is already catched elsewhere
            ) as parameter_unit_only_punctuation,   -- "some punctuation" is allowed for example mg/g so will leave that out for now

        (SELECT count(distinct parameter_unit) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE parameter_unit ~ '(,)'  -- usually something is wrong when commas are present
          and parameter_unit !~ '(^\W+$)' -- 'only punctuation' elsewhere
          and parameter_unit !~ '(\d\d\.\d\d\.\d\d)'  -- date elsewhere
          and parameter_unit !~ '(\d\d:\d\d)'  -- time elsewhere
          and parameter_unit !~ '(;)' -- semicolon elsewhere
            ) as parameter_unit_contains_comma,


        (SELECT count(distinct parameter_unit) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE parameter_unit ~ '(;)'  -- usually something is wrong when semicolons are present
          and parameter_unit !~ '(^\W+$)' -- 'only punctuation' elsewhere
          and parameter_unit !~ '(\d\d\.\d\d\.\d\d)'  -- date elsewhere
          and parameter_unit !~ '(\d\d:\d\d)'  -- time elsewhere
          and parameter_unit !~ '(,)' -- comma elsewhere
            ) as parameter_unit_contains_semicolon,

       (SELECT count(distinct parameter_unit) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE parameter_unit !~ '^ \w(\w|\d|\W)' AND -- sometimes the space before unit is extracted which is not a super critical mistake
              (parameter_unit ~ '^\W(\w|\d|\W)'
                   or parameter_unit ~ '^ \W(\w|\d|\W)')) -- mostly when the extracted field begins with punctuation it is not right
             as parameter_unit_begins_with_punct,


       (SELECT count(distinct parameter_unit) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE length(parameter_unit) > 15  -- arbitrarily chosen number for start
            ) as parameter_unit_over_15char,

---------------------------------------------------------------------------
       -- parameter_unit_raw
---------------------------------------------------------------------------

        -- whitespace
        (SELECT count(distinct parameter_unit_raw) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE parameter_unit_raw ~ '(^\s*$)') as parameter_unit_raw_only_whitespace,

              -- contains timestamp
       (SELECT count(distinct parameter_unit_raw) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE parameter_unit_raw ~'(\d\d:\d\d)'
                and parameter_unit_raw !~ '(\d\d\.\d\d\.\d\d)' -- no date here
           ) as parameter_unit_raw_contains_time,

       -- date
       (SELECT count(distinct parameter_unit_raw) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE parameter_unit_raw ~ '(\d\d\.\d\d\.\d\d)'
              and parameter_unit_raw !~'(\d\d:\d\d)' -- no time here
           ) as parameter_unit_raw_contains_date,


       (SELECT count(distinct parameter_unit_raw) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE  parameter_unit_raw ~ '(^\d*$)'
            and parameter_unit_raw !~ '(^\s*$)'
           ) as parameter_unit_raw_only_digits,

        (SELECT count(distinct parameter_unit_raw) FROM work.run_202104231122_analysis_texts_structured_cleaned
            WHERE parameter_unit_raw ~ '(\d)'
            and parameter_unit_raw !~ '(^\d*$)'
            and parameter_unit_raw !~ '([[:punct:]])' -- only digits no punctuation
           ) as parameter_unit_raw_some_digits,

        (SELECT count(distinct parameter_unit_raw) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE parameter_unit_raw ~ '(^\W+$)'  -- puctuation with some whitespace
          and parameter_unit_raw !~ '(^%$)'  -- % is allowed
          and parameter_unit_raw !~ '(^\s*$)' -- only whitespace is already catched elsewhere
            ) as parameter_unit_raw_only_punctuation,   -- "some punctuation" is allowed for example mg/g so will leave that out for now

        (SELECT count(distinct parameter_unit_raw) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE parameter_unit_raw ~ '(,)'  -- usually something is wrong when commas are present
          and parameter_unit_raw !~ '(^\W+$)' -- 'only punctuation' elsewhere
          and parameter_unit_raw !~ '(\d\d\.\d\d\.\d\d)'  -- date elsewhere
          and parameter_unit_raw !~ '(\d\d:\d\d)'  -- time elsewhere
          and parameter_unit_raw !~ '(;)' -- semicolon elsewhere
            ) as parameter_unit_raw_contains_comma,


        (SELECT count(distinct parameter_unit_raw) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE parameter_unit_raw ~ '(;)'  -- usually something is wrong when semicolons are present
          and parameter_unit_raw !~ '(^\W+$)' -- 'only punctuation' elsewhere
          and parameter_unit_raw !~ '(\d\d\.\d\d\.\d\d)'  -- date elsewhere
          and parameter_unit_raw !~ '(\d\d:\d\d)'  -- time elsewhere
          and parameter_unit_raw !~ '(,)' -- comma elsewhere
            ) as parameter_unit_raw_contains_semicolon,


        (SELECT count(distinct parameter_unit_raw) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE parameter_unit_raw !~ '^ \w(\w|\d|\W)' AND -- sometimes the space before unit is extracted which is not a super critical mistake
              (parameter_unit_raw ~ '^\W(\w|\d|\W)' or parameter_unit_raw ~ '^ \W(\w|\d|\W)')) -- mostly when the extracted field begins with punctuation it is not right
             as parameter_unit_raw_begins_with_punct,

       (SELECT count(distinct parameter_unit_raw) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE length(parameter_unit_raw) > 15  -- arbitrarily chosen number for start
            ) as parameter_unit_raw_over_15char,


---------------------------------------------------------------------------
       -- effective time üle 1990 ja alla 2020
 ---------------------------------------------------------------------------
       (select count(distinct effective_time) from work.run_202104231122_analysis_texts_structured_cleaned
           WHERE effective_time <= '1990-01-01'::date) as dates_before_1990,
       (select count(distinct effective_time) from work.run_202104231122_analysis_texts_structured_cleaned
           WHERE effective_time >= '2020-01-01'::date) as dates_after_2020,

---------------------------------------------------------------------------
-- value types
---------------------------------------------------------------------------

       -- value_type range
        (select count(distinct value_type) from work.run_202104231122_analysis_texts_structured_cleaned
          WHERE value_type in ('range')) as value_type_is_range,
       -- value type null
       (select count(distinct value_type) from work.run_202104231122_analysis_texts_structured_cleaned
          WHERE value_type is null) as value_type_is_null,
        --text
        (SELECT count(distinct value_type) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE value is not null and (value_type like 'text')) as value_type_is_text
                                                                                )

UNION ALL

(SELECT

    'count all rows' as type,

---------------------------------------------------------------------------
         -- kogu ridade arv
---------------------------------------------------------------------------

       cast((select count(*) from work.run_202104231122_analysis_texts_structured_cleaned) as numeric)
                                                            as all_entries,
---------------------------------------------------------------------------
        -- analysis ja parameter name ei tohi korraga nullid või tühjad olla, checkida seda
---------------------------------------------------------------------------
       (select count(*) from work.run_202104231122_analysis_texts_structured_cleaned
          WHERE (analysis_name is null AND parameter_name is null)
                    OR (analysis_name ~ '(^\s*$)' AND parameter_name is null)
                    OR (analysis_name is null AND parameter_name ~ '(^\s*$)')
                    OR (analysis_name ~ '(^\s*$)' AND parameter_name ~ '(^\s*$)')) as analysis_parameter_both_null,

---------------------------------------------------------------------------
       -- analysis name
---------------------------------------------------------------------------
        -- 'anonym' hulk analysis name hulgas
       (select count(analysis_name)
       from work.run_202104231122_analysis_texts_structured_cleaned
           WHERE analysis_name LIKE '%anonym%'
              or analysis_name LIKE '%ANONYM%') as analysis_name_anonym,
       -- whitespace
       (SELECT count(*) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE analysis_name ~ '(^\s*$)') as analysis_name_only_whitespace,
        -- digit
       (SELECT count(*) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE analysis_name ~ '(^\d*$)'
           and analysis_name !~ '(^\s*$)') as analysis_name_only_digits,
       --
       (SELECT count(*) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE analysis_name ~ '(\d)'
           and analysis_name !~ '(^\d*$)'
          and analysis_name !~ '([[:punct:]])' -- only looking entries with no punctuation
          and analysis_name not like 'Hemogramm 5-Osalise Leukogrammiga') as analysis_name_some_digits,

       -- punctuation
        (SELECT count(*) FROM work.run_202104231122_analysis_texts_structured_cleaned
       WHERE analysis_name ~ '(^\W+$)') as analysis_name_only_punctuation,

       -- tavaliselt kui sisalduvad mingid kirjavahemärgid analysis names on asi veits kahtlane
       -- kuigi mõnikord sidekriips - on normaalne, siis pigem peab siin eraldi ignoreerima keisse kus ta on normaalne
       -- sest ta on sageli ka situatsioonides kus on eraldus jama

       -- Vb peaks some_punctuation splittima kaheks - some punctuation võtta - sisaldused ära ja teha
       -- eraldi tulp et palju on - sees kuskil


        (SELECT count(*) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE analysis_name ~ '([[:punct:]])'
            and analysis_name !~ '(^[[:punct:]]$)'
            and analysis_name !~ '(\d)' -- only punctuation no numbers
            and analysis_name not like 'Hemogramm 5-Osalise Leukogrammiga') as analysis_name_some_punctuation,


       (SELECT count(*) FROM work.run_202104231122_analysis_texts_structured_cleaned
       WHERE analysis_name ~ '([[:punct:]])'
          and analysis_name ~ '(\d)'
         and analysis_name !~'(\d\d:\d\d)' -- excluding time
         and analysis_name !~ '(\d\d\.\d\d\.\d\d)' -- excluding date
            and analysis_name not like 'Hemogramm 5-Osalise Leukogrammiga') as analysis_name_punctuation_and_digits,


    -- contains timestamp
       (SELECT count(*) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE analysis_name ~'(\d\d:\d\d)'
                and analysis_name !~ '(\d\d\.\d\d\.\d\d)' -- no date here
           ) as analysis_name_contains_time,

       (SELECT count(*) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE analysis_name ~ '(\d\d\.\d\d\.\d\d)'
              and analysis_name !~'(\d\d:\d\d)' -- no time here
           ) as analysis_name_contains_date,

       (SELECT count(*) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE analysis_name ~ '(^\D$)'
          and analysis_name !~ '([[:punct:]])' ) as analysis_name_only_one_letter,

---------------------------------------------------------------------------
       -- parameter_name
---------------------------------------------------------------------------

       (select count(parameter_name)
       from work.run_202104231122_analysis_texts_structured_cleaned
           WHERE parameter_name LIKE '%anonym%'
              or parameter_name LIKE '%ANONYM%'
           ) as parameter_name_anonym,
       -- whitespace
        (SELECT count(*) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE parameter_name ~ '(^\s*$)'
            ) as parameter_name_only_whitespace,
        -- digit
        (SELECT count(*) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE parameter_name ~ '(^\d*$)'
            and parameter_name !~ '(^\s*$)'
            ) as parameter_name_only_digits,

       (SELECT count(*) FROM work.run_202104231122_analysis_texts_structured_cleaned
            WHERE parameter_name ~ '(\d)'
            and parameter_name !~ '(^\d*$)'
            and parameter_name !~ '([[:punct:]])' -- only digits no punctuation
           ) as parameter_name_some_digits,

       -- punctuation
        (SELECT count(*) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE parameter_name ~ '(^\W+$)'
            ) as parameter_name_only_punctuation,

        (SELECT count(*) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE parameter_name ~ '([[:punct:]])'
            and parameter_name !~ '(^[[:punct:]]$)'
            and parameter_name !~ '(\d)' -- only punctuation no numbers
         ) as parameter_name_some_punctuation,

       (SELECT count(*) FROM work.run_202104231122_analysis_texts_structured_cleaned
       WHERE parameter_name ~ '([[:punct:]])'
          and parameter_name ~ '(\d)'
         and parameter_name !~'(\d\d:\d\d)' -- excluding time
         and parameter_name !~ '(\d\d\.\d\d\.\d\d)' -- excluding date
           ) as parameter_name_punctuation_and_digits,


       -- contains timestamp
       (SELECT count(*) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE parameter_name ~'(\d\d:\d\d)'
                and parameter_name !~ '(\d\d\.\d\d\.\d\d)' -- no date here
           ) as parameter_name_contains_time,

       (SELECT count(*) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE parameter_name ~ '(\d\d\.\d\d\.\d\d)'
              and parameter_name !~'(\d\d:\d\d)' -- no time here
           ) as parameter_name_contains_date,


       (SELECT count(*) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE parameter_name ~ '(^\D$)'
          and parameter_name !~ '([[:punct:]])' ) as parameter_name_only_one_letter,






---------------------------------------------------------------------------
        -- parameter_unit
---------------------------------------------------------------------------
        --whitespace

        (SELECT count(*) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE parameter_unit ~ '(^\s*$)') as parameter_unit_only_whitespace,

              -- contains timestamp
       (SELECT count(*) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE parameter_unit ~'(\d\d:\d\d)'
                and parameter_unit !~ '(\d\d\.\d\d\.\d\d)' -- no date here
           ) as parameter_unit_contains_time,

       (SELECT count(*) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE parameter_unit ~ '(\d\d\.\d\d\.\d\d)'
              and parameter_unit !~'(\d\d:\d\d)' -- no time here
           ) as parameter_unit_contains_date,


       (SELECT count(*) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE  parameter_unit ~ '(^\d*$)'
            and parameter_unit !~ '(^\s*$)'
           ) as parameter_unit_only_digits,

        (SELECT count(*) FROM work.run_202104231122_analysis_texts_structured_cleaned
            WHERE parameter_unit ~ '(\d)'
            and parameter_unit !~ '(^\d*$)'
            and parameter_unit !~ '([[:punct:]])' -- only digits no punctuation
           ) as parameter_unit_some_digits,

        (SELECT count(*) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE parameter_unit ~ '(^\W+$)'  -- puctuation with some whitespace
          and parameter_unit !~ '(^%$)'  -- % is allowed
          and parameter_unit !~ '(^\s*$)' -- only whitespace is already catched elsewhere
            ) as parameter_unit_only_punctuation,   -- "some punctuation" is allowed for example mg/g so will leave that out for now

        (SELECT count(*) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE parameter_unit ~ '(,)'  -- usually something is wrong when commas are present
          and parameter_unit !~ '(^\W+$)' -- 'only punctuation' elsewhere
          and parameter_unit !~ '(\d\d\.\d\d\.\d\d)'  -- date elsewhere
          and parameter_unit !~ '(\d\d:\d\d)'  -- time elsewhere
          and parameter_unit !~ '(;)' -- semicolon elsewhere
            ) as parameter_unit_contains_comma,


        (SELECT count(*) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE parameter_unit ~ '(;)'  -- usually something is wrong when semicolons are present
          and parameter_unit !~ '(^\W+$)' -- 'only punctuation' elsewhere
          and parameter_unit !~ '(\d\d\.\d\d\.\d\d)'  -- date elsewhere
          and parameter_unit !~ '(\d\d:\d\d)'  -- time elsewhere
          and parameter_unit !~ '(,)' -- comma elsewhere
            ) as parameter_unit_contains_semicolon,

       (SELECT count(*) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE parameter_unit !~ '^ \w(\w|\d|\W)' AND -- sometimes the space before unit is extracted which is not a super critical mistake
              (parameter_unit ~ '^\W(\w|\d|\W)'
                   or parameter_unit ~ '^ \W(\w|\d|\W)')) -- mostly when the extracted field begins with punctuation it is not right
             as parameter_unit_begins_with_punct,


       (SELECT count(*) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE length(parameter_unit) > 15  -- arbitrarily chosen number for start
            ) as parameter_unit_over_15char,

---------------------------------------------------------------------------
       -- parameter_unit_raw
---------------------------------------------------------------------------

        -- whitespace
        (SELECT count(*) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE parameter_unit_raw ~ '(^\s*$)') as parameter_unit_raw_only_whitespace,

              -- contains timestamp
       (SELECT count(*) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE parameter_unit_raw ~'(\d\d:\d\d)'
                and parameter_unit_raw !~ '(\d\d\.\d\d\.\d\d)' -- no date here
           ) as parameter_unit_raw_contains_time,

       -- date
       (SELECT count(*) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE parameter_unit_raw ~ '(\d\d\.\d\d\.\d\d)'
              and parameter_unit_raw !~'(\d\d:\d\d)' -- no time here
           ) as parameter_unit_raw_contains_date,


       (SELECT count(*) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE  parameter_unit_raw ~ '(^\d*$)'
            and parameter_unit_raw !~ '(^\s*$)'
           ) as parameter_unit_raw_only_digits,

        (SELECT count(*) FROM work.run_202104231122_analysis_texts_structured_cleaned
            WHERE parameter_unit_raw ~ '(\d)'
            and parameter_unit_raw !~ '(^\d*$)'
            and parameter_unit_raw !~ '([[:punct:]])' -- only digits no punctuation
           ) as parameter_unit_raw_some_digits,

        (SELECT count(*) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE parameter_unit_raw ~ '(^\W+$)'  -- puctuation with some whitespace
          and parameter_unit_raw !~ '(^%$)'  -- % is allowed
          and parameter_unit_raw !~ '(^\s*$)' -- only whitespace is already catched elsewhere
            ) as parameter_unit_raw_only_punctuation,   -- "some punctuation" is allowed for example mg/g so will leave that out for now

        (SELECT count(*) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE parameter_unit_raw ~ '(,)'  -- usually something is wrong when commas are present
          and parameter_unit_raw !~ '(^\W+$)' -- 'only punctuation' elsewhere
          and parameter_unit_raw !~ '(\d\d\.\d\d\.\d\d)'  -- date elsewhere
          and parameter_unit_raw !~ '(\d\d:\d\d)'  -- time elsewhere
          and parameter_unit_raw !~ '(;)' -- semicolon elsewhere
            ) as parameter_unit_raw_contains_comma,


        (SELECT count(*) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE parameter_unit_raw ~ '(;)'  -- usually something is wrong when semicolons are present
          and parameter_unit_raw !~ '(^\W+$)' -- 'only punctuation' elsewhere
          and parameter_unit_raw !~ '(\d\d\.\d\d\.\d\d)'  -- date elsewhere
          and parameter_unit_raw !~ '(\d\d:\d\d)'  -- time elsewhere
          and parameter_unit_raw !~ '(,)' -- comma elsewhere
            ) as parameter_unit_raw_contains_semicolon,


        (SELECT count(*) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE parameter_unit_raw !~ '^ \w(\w|\d|\W)' AND -- sometimes the space before unit is extracted which is not a super critical mistake
              (parameter_unit_raw ~ '^\W(\w|\d|\W)' or parameter_unit_raw ~ '^ \W(\w|\d|\W)')) -- mostly when the extracted field begins with punctuation it is not right
             as parameter_unit_raw_begins_with_punct,

       (SELECT count(*) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE length(parameter_unit_raw) > 15  -- arbitrarily chosen number for start
            ) as parameter_unit_raw_over_15char,


---------------------------------------------------------------------------
       -- effective time üle 1990 ja alla 2020
 ---------------------------------------------------------------------------
       (select count(*) from work.run_202104231122_analysis_texts_structured_cleaned
           WHERE effective_time <= '1990-01-01'::date) as dates_before_1990,
       (select count(*) from work.run_202104231122_analysis_texts_structured_cleaned
           WHERE effective_time >= '2020-01-01'::date) as dates_after_2020,

---------------------------------------------------------------------------
-- value types
---------------------------------------------------------------------------

       -- value_type range
        (select count(*) from work.run_202104231122_analysis_texts_structured_cleaned
          WHERE value_type in ('range')) as value_type_is_range,
       -- value type null
       (select count(*) from work.run_202104231122_analysis_texts_structured_cleaned
          WHERE value_type is null) as value_type_is_null,
        --text
        (SELECT count(*) FROM work.run_202104231122_analysis_texts_structured_cleaned
        WHERE value is not null and (value_type like 'text')) as value_type_is_text)

;

-- CREATE TEMP TABLE tests_initial2 AS
    SELECT * FROM tests_initial

    UNION ALL


    (SELECT '% all rows from total' as type,
           all_entries/all_entries*100 as all_entries,
       round(analysis_parameter_both_null/all_entries*100,2) as analysis_parameter_both_null,
       round(analysis_name_anonym/all_entries*100,2) as analysis_name_anonym,
        round(analysis_name_only_whitespace/all_entries*100,2) as analysis_name_only_whitespace,
           round(analysis_name_only_digits/all_entries*100,2) as analysis_name_only_digits,
           round(analysis_name_some_digits/all_entries*100,2) as analysis_name_some_digits,
           round(analysis_name_only_punctuation/all_entries*100,2) as analysis_name_only_punctuation,
           round(analysis_name_some_punctuation/all_entries*100,2) as analysis_name_some_punctuation,
           round(analysis_name_punctuation_and_digits/all_entries*100,2) as analysis_name_punctuation_and_digits,
           round(analysis_name_contains_time/all_entries*100,2) as analysis_name_contains_time,
           round(analysis_name_contains_date/all_entries*100,2) as analysis_name_contains_date,
           round(analysis_name_only_one_letter/all_entries*100,2) as analysis_name_only_one_letter,
           round(parameter_name_anonym/all_entries*100,2) as parameter_name_anonym,
           round(parameter_name_only_whitespace/all_entries*100,2) as parameter_name_only_whitespace,
           round(parameter_name_only_digits/all_entries*100,2) as parameter_name_only_digits,
           round(parameter_name_some_digits/all_entries*100,2) as parameter_name_some_digits,

           round(parameter_name_only_punctuation/all_entries*100,2) as parameter_name_only_punctuation,
           round(parameter_name_some_punctuation/all_entries*100,2) as parameter_name_some_punctuation,
           round(parameter_name_punctuation_and_digits/all_entries*100,2) as parameter_name_punctuation_and_digits,
           round(parameter_name_contains_time/all_entries*100,2) as parameter_name_contains_time,
           round(parameter_name_contains_date/all_entries*100,2) as parameter_name_contains_date,
           round(parameter_name_only_one_letter/all_entries*100,2) as parameter_name_only_one_letter,

           round(parameter_unit_only_whitespace/all_entries*100,2) as parameter_unit_only_whitespace,
           round(parameter_unit_contains_time/all_entries*100,2) as parameter_unit_contains_time,
           round(parameter_unit_contains_date/all_entries*100,2) as parameter_unit_contains_date,
           round(parameter_unit_only_digits/all_entries*100,2) as parameter_unit_only_digits,
           round(parameter_unit_some_digits/all_entries*100,2) as parameter_unit_some_digits,
           round(parameter_unit_only_punctuation/all_entries*100,2) as parameter_unit_only_punctuation,
           round(parameter_unit_contains_comma/all_entries*100,2) as parameter_unit_contains_comma,
           round(parameter_unit_contains_semicolon/all_entries*100,2) as parameter_unit_contains_semicolon,
           round(parameter_unit_begins_with_punct/all_entries*100,2) as parameter_unit_begins_with_punct,
           round(parameter_unit_over_15char/all_entries*100,2) as parameter_unit_over_15char,

           round(parameter_unit_raw_only_whitespace/all_entries*100,2) as parameter_unit_raw_only_whitespace,
           round(parameter_unit_raw_contains_time/all_entries*100,2) as parameter_unit_raw_contains_time,
           round(parameter_unit_raw_contains_date/all_entries*100,2) as parameter_unit_raw_contains_date,
           round(parameter_unit_raw_only_digits/all_entries*100,2) as parameter_unit_raw_only_digits,
           round(parameter_unit_raw_some_digits/all_entries*100,2) as parameter_unit_raw_some_digits,
           round(parameter_unit_raw_only_punctuation/all_entries*100,2) as parameter_unit_raw_only_punctuation,
           round(parameter_unit_raw_contains_comma/all_entries*100,2) as parameter_unit_raw_contains_comma,
           round(parameter_unit_raw_contains_semicolon/all_entries*100,2) as parameter_unit_raw_contains_semicolon,
           round(parameter_unit_raw_begins_with_punct/all_entries*100,2) as parameter_unit_raw_begins_with_punct,
           round(parameter_unit_raw_over_15char/all_entries*100,2) as parameter_unit_raw_over_15char,

           round(dates_before_1990/all_entries*100,2) as dates_before_1990,
           round(dates_after_2020/all_entries*100,2) as dates_after_2020,
           round(value_type_is_range/all_entries*100,2) as value_type_is_range,
           round(value_type_is_null/all_entries*100,2) as value_type_is_null,
           round(value_type_is_text/all_entries*100 ,2) as value_type_is_text
    FROM tests_initial WHERE type LIKE 'count all rows')
   ;
