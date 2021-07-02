
## Detailed information about matching

### Step 1 -  Unresolved tables

Create tables of unresolved row id's ` *_unresolved_entry` , `*_unresolved_html`.
Every time some rows are matched during next steps we remove those rows from unresolved.

#### Example table
Table: ` *_unresolved_entry` 

id|
--|
64519|
17269|
16338|
16377|

### Step 2 - Perfect Matches

Finds perfect matches and adds them to ` *_matched table` .

Matching is perfect if all columns that are both in entry and html are exactly the same:
* epi_id (E)
* analysis_name (AN)
* parameter_name (P)
* parameter_unit (PU)
* effective_time (D)
* value (V)
* reference_values (R )
 
` match description ` is `analysis_name, parameter_name, parameter_unit, effective_time, value, reference_values`.

### Step 3 - Unique Matches

Finds unique rows in html and entry (rows that are only in either html or entry).  Inserts them to ` *_matched table` .

If  `epi_id` exists only in html table, all the rows with that `epi_id` are considered as unique for html.
If  `epi_id` exists only in entry table, all the rows with that `epi_id` are considered as unique for entry.

` match description ` is either `only_html `or `only_entry`.

Finally removes those rows from unresolved tables.

### Step 4 -Matching VEDP

Creates matching table between html and entry columns 
* `value`
* `epi_id`
* `effective_time`
* `parameter_name`.

Note: does not add them to  ` *_matched table`  yet!
Note: only looks at rows that are unresolved.


### Step 5 -Matching VEDPU

Creates matching table between html and entry columns `value`, `epi_id`, `effective_time`, `parameter_name`, `parameter_unit`.

Matches tables if
* V, E, D, P are exacty the same and
	- `parameter_unit` columns are exactly the same or 
	- `html.parameter_unit` is NULL and  `entry.parameter_unit` is not NULL or 
	- `entry.parameter_unit` is NULL and  `html.parameter_unit` is not NULL or
	- `entry.parameter_unit` is NULL and  `html.parameter_unit` is NULL

To the ` *_matched` table only rows which have unique matching are added.


### Step6 - Adding rows matched by VEDP

Adds rows that were matched by 
* `value`
* `epi_id`
* `effective_time`
* `parameter_name`
to the  ` *_matched` table. 

Note: only looks at rows that are unresolved.

### Step7 - Detect Ties
During cleaning column `value_raw`  important information is lost  which makes it impossible to match some rows. 

Those rows are detected and marked  as follows:
match_description = `html ties, parameter_name is not cleaned correctly` or  `entry ties, parameter_name is not cleaned correctly`.

Note: only looks at rows that are unresolved.

#### Example 
Example: epi_id = '19289924'

Html table
id| epi_id | parameter_name_raw | parameter_name | value | effective_time | ...
--|--|--|--|--|--|--
5| 19289924 | a5045 AaDpO2 | AaDpO2 | 101.3 | 2014-05-07 00:00:00.000000  
6| 19289924 | a5045 AaDpO2 (t) | AaDpO2 | 101.3 | 2014-05-07 00:00:00.000000

Correspondig entry table 
id | epi_id | parameter_name_raw | parameter_name | value | effective_time | ... 
--|--|--|--|--|--|-- 
10| 19289924 | AaDpO2 | AaDpO2 | 101.3 | 2014-05-07 00:00:00.000000  
11| 19289924 | AaDpO2 (t) | AaDpO2 | 101.3 | 2014-05-07 00:00:00.000000
  
  Matching is done by **NOT** raw columns, so there is no unique matching:
 id 5 can be matched with 10  
id 5 can be matched with id 11  
id 6 can be matched with id 10  
id 6 can be matched with id 11

If matching would be done by **_raw** column, then the matching would be unique:
id 5 matched with d 10  
id 6 matched with id 11

TO BE CONTINUED

### Matching EPD

### Matching VEP

### Matching VED

### Add unmatched to matched table
