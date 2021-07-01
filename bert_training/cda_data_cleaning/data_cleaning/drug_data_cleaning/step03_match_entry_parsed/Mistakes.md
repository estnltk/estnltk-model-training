

# Known problems that occur during matching

## Not all duplicates are deleted.

The problem is explained through an example.

Supppose we have parsed and entry table like following.

**Parsed:**

epi_id | drug_code| recipe_code| drug_code_display_name | dose_quantity_value | dose_quantity_unit | rate_quantity_value | rate_quantity_unit |  
--|--|--|--|--|--|--|--|
123 | N06AA09 |10 | amitriptüliin | 1 | tablett| 2 | korda   
123 | N06AA09 |11 | amitriptüliin | 1 | tablett|  |   
123 | N06AA09 |12 | amitriptüliin | 1 | tablett|  |   
123 | N06AA09 |13 |amitriptüliin | 1 | tablett|  |  

**Entry:**

epi_id | drug_code | drug_code_display_name | dose_quantity_value | dose_quantity_unit | rate_quantity_value | rate_quantity_unit | 
--|--|--|--|--|--|--|
123 | N06AA09 |amitriptüliin | 10 | mg|  | 
123 | N06AA09 |amitriptüliin | 25 | mg|  | 
123 | N06AA09 |amitriptüliin | 25 | mg|  | 
123 | N06AA09 |amitriptüliin | 25 | mg|  | 

It is clear that those rows are equivalent, the dose is just measured in different units. Most likely 2-4 rows are **not** duplicates because they have different recipe codes. 

During the matching step rows are matched according to  `epi_id`, `active_ingredient/drug_code_display_name` and `drug_code`.

The result (**with duplicates**) has 4**2 = 16 rows. 

epi_id | drug_code |recipe_code| ingredient | dose_quantity_value_entry | dose_quantity_value_parsed |  dose_quantity_unit_entry |   dose_quantity_unit_parsed |  rate_quantity_value_entry |rate_quantity_value_parsed  | rate_quantity_unit_entry | rate_quantity_unit_parsed | 
--|--|--|--|--|--|--|--|--|--|--|--|
123|N06AA09|10|amitriptüliin|25.0000|1|mg|tablett||2||korda|
123|N06AA09|11|amitriptüliin|25.0000|1|mg|tablett|||||
123|N06AA09|12|amitriptüliin|25.0000|1|mg|tablett|||||
123|N06AA09|13|amitriptüliin|25.0000|1|mg|tablett|||||
123|N06AA09|10|amitriptüliin|10.0000|1|mg|tablett||2||korda|
123|N06AA09|11|amitriptüliin|10.0000|1|mg|tablett|||||
123|N06AA09|12|amitriptüliin|10.0000|1|mg|tablett|||||
123|N06AA09|13|amitriptüliin|10.0000|1|mg|tablett|||||
123|N06AA09|10|amitriptüliin|25.0000|1|mg|tablett||2||korda|
123|N06AA09|11|amitriptüliin|25.0000|1|mg|tablett|||||
123|N06AA09|12|amitriptüliin|25.0000|1|mg|tablett|||||
123|N06AA09|13|amitriptüliin|25.0000|1|mg|tablett|||||
123|N06AA09|10|amitriptüliin|25.0000|1|mg|tablett||2||korda|
123|N06AA09|11|amitriptüliin|25.0000|1|mg|tablett|||||
123|N06AA09|12|amitriptüliin|25.0000|1|mg|tablett|||||
123|N06AA09|13|amitriptüliin|25.0000|1|mg|tablett|||||


Now using **distinct** in postgres the obtained table removes the dupliciated, but not all. Instead of expected 4 rows, it returns 8 as row with dose 10 is .

epi_id | drug_code |recipe_code| ingredient | dose_quantity_value_entry | dose_quantity_value_parsed |  dose_quantity_unit_entry |   dose_quantity_unit_parsed |  rate_quantity_value_entry |rate_quantity_value_parsed  | rate_quantity_unit_entry | rate_quantity_unit_parsed | 
--|--|--|--|--|--|--|--|--|--|--|--|
123|N06AA09|10|amitriptüliin|10.0000|1|mg|tablett||2|
123|N06AA09|11|amitriptüliin|10.0000|1|mg|tablett|||
123|N06AA09|12|amitriptüliin|10.0000|1|mg|tablett|||
123|N06AA09|13|amitriptüliin|10.0000|1|mg|tablett|||
123|N06AA09|10|amitriptüliin|25.0000|1|mg|tablett||2|
123|N06AA09|11|amitriptüliin|25.0000|1|mg|tablett|||
123|N06AA09|12|amitriptüliin|25.0000|1|mg|tablett|||
123|N06AA09|13|amitriptüliin|25.0000|1|mg|tablett|||






