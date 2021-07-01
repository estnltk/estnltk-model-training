from collections import defaultdict

# if nothing else is specified, these fields will be extracted to the texts collection
from typing import DefaultDict

DEFAULT_FIELD_EXTRACTION_PATTERN = {
    "allergy_entry": ["original_text", "material"],
    "allergy": ["text"],
    "anamnesis": ["anamsum", "anamnesis", "diagnosis", "dcase"],
    "death": ["text"],
    "objective_finding": ["text"],
    "procedures_entry": ["text"],
    "procedures": ["text"],
    "summary": ["sumsum", "drug", "prc"],
    "surgery_entry": ["text"],
    "surgery": ["text"],
}

# extracted fields
EXTRACTABLE_TABLE_FIELDS: DefaultDict = defaultdict(lambda: DEFAULT_FIELD_EXTRACTION_PATTERN)


map_table_to_effective_time_field = {
    "allergy_entry": "effective_time",
    "death": "death_time",
    "procedures_entry": "effective_time",
    "surgery_entry": "effective_time",
}
