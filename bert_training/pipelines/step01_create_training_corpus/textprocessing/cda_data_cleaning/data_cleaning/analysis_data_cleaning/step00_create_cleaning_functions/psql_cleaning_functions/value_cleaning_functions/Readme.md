
# Value cleaning functions

Create value cleaning functions to postgresql.

NB! Do not know how to clean values like:
* 1-2-3
* 0-1-2-5

Cleaning is performed according to the type of value. There are 9 different value types:
* float
* integer
* text
* ratio
* range
* number and parentheses
* time series
* text and value
* time series

Just detects type but doesn't clean:
* text and value, e.g Neg(<3.0)
* text

Value cleaning functions  are based on [common/value.py](https://git.stacc.ee/project4/cda-data-cleaning/blob/master/common/value.py). Do not include `DrugDataValue` functions!

# Types

All functions remove whitespaces from beginning and end.

## Float 
Value seperator is changed from comma to a dot. If value starts with plus, then plus is removed.  Missing zeros are added.

value_raw| value
--|--
3,2|3.2
+3.2 | 3.2
3\. | 3.0
\.3 | 0.3
-3,4|-3.5

## Integer 
Removes plus sign from beginning.

value_raw| value
--|--
+4 | 4

## Text
Value is considered text when it contains only charachters. No cleaning is actually performed as it is not known how to do it.
Still some parts could be unified in the future. For example 'NEG', 'Negat', 'Negatiivne', etc should be cleaned to have one value.


## Range


  value_raw| value|
--|--|
<=3 |(-Inf;3] |  
\<3| (-Inf;3)  |
\>=3 | [3;Inf)  |
\>3 | (3;Inf) |
2-4 | [2,4] |
-2-4 | [-2,4] |
-4--2 | [-4,-2] |
2...4| [2,4]


## Ratio
If value has signs ':' or '/' then it is considered a ratio. All ratios are unified to be seperated with '/'.

value_raw| value
--|--
3:4 | 3/4


## Number and parentheses

If value is both seperately and in parentheses remove the extra value.

value_raw| value
--|--
3(3) | 3
3.0(3.0)|3.0
3.0(3)|3.0

## Time series

Before value cleaning is applied (e.g. table `*_analysis_html`):

epi_id | parameter_name_raw| value_raw
-- | -- | --
10022062	|Troponiin I, kardiaalne	|13.31(19:45),11.361(22:47),
10022062	|P-CK-MBm	|9.2(22:47),11.1(19:45),
10022062	|B-MCH	|24.2(19:45),24.6(22:46),
10022062	|B-MCHC|	319(19:45),320(22:46),318(23:02)

	
After value cleaning has been applied (e.g. table `*_analysis_html_cleaned`).

epi_id | parameter_name_raw | value_type | value_raw | value | time_series_block_id 
-------|------------|-----|------|-------|--------------------
10022062 | Troponiin I, kardiaalne | time_series |13.31(19:45),11.361(22:47) | 13.31 | 1
10022062	| Troponiin I, kardiaalne |time_series	|13.31(19:45),11.361(22:47),	|11.361	|1
10022062	|P-CK-MBm	|time_series	|9.2(22:47),11.1(19:45), | 9.2	|2
10022062	|P-CK-MBm	|time_series	|9.2(22:47),11.1(19:45), |11.1	|2
10022062	|B-MCH	|time_series	|24.2(19:45),24.6(22:46),|24.2	|3
10022062	|B-MCH	|time_series	|24.2(19:45),24.6(22:46),|24.6	|3
10022062	|B-MCHC|time_series	|319(19:45),320(22:46),318(23:02), |319	|4
10022062	|B-MCHC|time_series	|319(19:45),320(22:46),318(23:02), |320	|4
10022062	|B-MCHC|time_series	|319(19:45),320(22:46),318(23:02), |318 |4


## Text and value - NOT CLEANED

value_raw| value
--|--
B(13:21),B(13:21),|B(13:21),B(13:21),
neg(0.106)|neg(0.106)





