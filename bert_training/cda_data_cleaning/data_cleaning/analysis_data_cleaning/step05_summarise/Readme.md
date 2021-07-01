# Summarise `analysis_cleaned`

This is the final step of the pipeline.
It gives a summary of the final output table `analysis_cleaned`.

## Output
The summary is written into a csv file (`<prefix>_analysis_cleaned_summary.csv)`
in the folder `summaries`.

The same table is also written to the database as `<prefix>_analysis_cleaned_summary`.

## Example

Example of the output table.

Column `selection_count` is the number of the rows that match the given criteria in the description.

Column `percentage` is the percentage out of all rows (`selection_count`/`number_of_rows`).


description | selection_count | percentage
----|----|-----
Number of rows: |	11433|	100
Number of matched entries: |	1 |	0
Number of entries with single source:|	11432	|100
Number of entries with measurement unit:|	8897|	78
Number of entries with measurement time:|	11417|	100
Number of unmatched entries:|	0	|0
Number of timeseries components:|	176	|2
Number of timeseries:	|79	|1
Number of theoretically unmatchable entries based on value:|	0	|0
Number of LOINCED entries with with time and measurement unit:|	8415	|74
Number of LOINCED entries:|	10317	|90
Number of theoretically unmatchable entries based on counts:	|	
