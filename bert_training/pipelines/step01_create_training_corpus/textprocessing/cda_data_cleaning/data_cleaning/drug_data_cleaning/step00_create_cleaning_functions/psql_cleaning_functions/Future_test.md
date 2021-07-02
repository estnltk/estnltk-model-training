# some examples for future tests for psql functions

Function: clean_active_ingredient_entry

select work.clean_active_ingredient_entry('teof�lliin', 'R03DA04'); -- expected: {teofülliin,null,null}
select work.clean_active_ingredient_entry('gl_koosamiin','M01AX05'); -- expected: {glükoosamiin,null,null}
select work.clean_active_ingredient_entry('asitrom_tsiin','J01FA10'); -- expected: {asitromütsiin,null,null}

select work.clean_active_ingredient_entry('metforal  500 tablett  500mg n120', NULL); -- expected:{metforal 500 tablett,500mg,n120}
select work.clean_active_ingredient_entry('spirix 50 mg tablett  50mg n20',NULL); -- expected:{spirix 50mg,50 mg tablett,n20}
select work.clean_active_ingredient_entry('betaloc zok retardtablett  50mg n30',NULL); -- expected:{betaloc zok retardtablett,50mg,n30}
select work.clean_active_ingredient_entry('gliklasiid 60.0 mg',NULL); -- expected:{gliklasiid,60.0 mg,null}
select work.clean_active_ingredient_entry('deksametasoon 1,0 mg 1,0 ml',NULL); -- expected:{deksametasoon,1,0 mg 1,0 ml,null}


Function: clean_dose_quantity_value
--some examples for future tests

select work.clean_dose_quantity_value(''); --{null, null}

select work.clean_dose_quantity_value('12mg'); --{12,mg}
select work.clean_dose_quantity_value('12 mg'); --{12,mg}
select work.clean_dose_quantity_value('12 mg 12'); --{12,mg}
select work.clean_dose_quantity_value('12 tü'); --{12,tü}

select work.clean_dose_quantity_value('12,5mg'); --{12,mg}
select work.clean_dose_quantity_value('12,5 mg'); --{12,mg}
select work.clean_dose_quantity_value('12,5 mg 12'); --{12,mg}
select work.clean_dose_quantity_value('12,5 tü'); --{12,tü}

select work.clean_dose_quantity_value('mg'); --{null, mg}
select work.clean_dose_quantity_value('1 ti 2 tk'); --{1,ti};
select work.clean_dose_quantity_value('5/1.25mg'); --{5/1.25,mg}
