# Databricks notebook source
# We read in admission type definitions and use them to write code that applies their hierarchical rules to a set of admissions

# COMMAND ----------

import io
import pandas as pd


# Converts a standard comma separated list string into regex union, formatting requirements safely allow for incomplete code stems to be used, i.e. "C1" for all codes starting with those two characters
def convert_to_regex_string(input_string, sep=","):
  return f"'{'|'.join(['^' + x for x in input_string.split(sep)] + [',' + x for x in input_string.split(sep)])}'"


# Programmatically builds a case string for an admission type
def create_case_string(row, sep=","):
  case_string = "WHEN "
  
  # Deal with special cases via the next few if statements, things like PIMS, Nosocomial etc. have date requirements as well as codes that are defined during admission identification
  if row['positive_covid_test_window']:
    case_string += "(POSITIVE_COVID_TEST_IN_WINDOW == 1) AND "

  if "PIMS" in row['name']:
    case_string += "(PIMS_REQUIREMENT == 1) AND "
    
  if "NOSOCOMIAL" in row['name']:
    if "EXCLUDE" in row['name']:
      case_string += "(NOSOCOMIAL_FLAG_ALT == 1) AND "
    else:
      case_string += "(NOSOCOMIAL_FLAG == 1) AND "
  
  # Every admission type requires the fundamental presence of COVID evidence, 'EXCLUDE' types are those that would have been included if we allowed secondary U073/U074 to be sufficient on their own
  if "EXCLUDE" in row['name']:
    case_string += "(COVID_OTHER_SECONDARY_HOSPITALISATION == 1) AND "
  else:
    case_string += "(PIMS_REQUIREMENT == 1 OR COVID_PRIMARY_HOSPITALISATION == 1 OR COVID_DEFINITE_SECONDARY_HOSPITALISATION == 1 OR POSITIVE_COVID_TEST_INCLUSION == 1) AND "

  # Using the codes in the four include/exclude columns, build up requirement statements for type definitions, where primary inclusion codes are present, note that neither primary nor secondary exclude codes can be present, similar for secondary
  primary_strings = []
  if row['includes_primary']:
    primary_strings.append(f"DIAG_4_CONCAT_PRIMARY RLIKE {convert_to_regex_string(row['includes_primary'], sep)}")
  if row['excludes_primary']:
    primary_strings.append(f"DIAG_4_CONCAT_PRIMARY NOT RLIKE {convert_to_regex_string(row['excludes_primary'], sep)}")
  if row['excludes_secondary']:
    primary_strings.append(f"DIAG_4_CONCAT_SECONDARY NOT RLIKE {convert_to_regex_string(row['excludes_secondary'], sep)}")
  primary_string = ' AND '.join(primary_strings) 
    
  secondary_strings = []
  if row['includes_secondary']:
    secondary_strings.append(f"DIAG_4_CONCAT_SECONDARY RLIKE {convert_to_regex_string(row['includes_secondary'], sep)}")
  if row['excludes_secondary']:
    secondary_strings.append(f"DIAG_4_CONCAT_SECONDARY NOT RLIKE {convert_to_regex_string(row['excludes_secondary'], sep)}")
  if row['excludes_primary']:
    secondary_strings.append(f"DIAG_4_CONCAT_PRIMARY NOT RLIKE {convert_to_regex_string(row['excludes_primary'], sep)}")
  secondary_string = ' AND '.join(secondary_strings)
  
  # When both primary and secondary include codes are present in a type definition, we allow inclusion based on either of the strings built above being satisified, if not then BOTH must be satisfied, i.e. if there were primary include + exclude codes AND secondary exclude codes we require all of them to be satisfied, see PIMS and INCIDENTAL defns to make sense of this
  if primary_string and secondary_string and row['includes_primary'] and row['includes_secondary']:
    case_string += f"(({primary_string}) OR ({secondary_string}))"
  elif primary_string and secondary_string and primary_string == secondary_string:
    case_string += f"({primary_string})"
  elif primary_string and secondary_string:
    case_string += f"(({primary_string}) AND ({secondary_string}))"
  elif primary_string:
    case_string += f"({primary_string})"
  elif secondary_string:
    case_string += f"({secondary_string})"
  else:
    case_string += "(TRUE)"
  
  case_string += f" THEN '{row['type']}'\n"
  return case_string


# Creates binary case when string for each type definition
def create_case_when_string(row):
  return f"CASE {row['case_statement'].split(' THEN ')[0]} THEN 1 ELSE 0 END AS {row['name']}"


# Creates an option in the overall TYPE_OF_ADMISSION categorical for each type definition
def create_categorical_definition_string(case_statements):
  return f"CASE\n      {case_statements.str.cat(sep='      ')}      ELSE 'Other Exclude'\n    END AS TYPE_OF_ADMISSION"


def create_binary_definitions_string(case_when_statements):
  return case_when_statements.str.cat(sep=',\n    ')


# Programmatically builds a SQL statement to apply the hierarchy of admission types via ICD-10 codes and other variables to a table of admissions
def create_typed_admissions_query(table_name):
  df = pd.read_csv("../../data/admission_type_definitions.csv").fillna(False)
  df['name'] = df['type'].str.replace(' ','_').str.upper()

  df["case_statement"] = df.apply(create_case_string, axis=1)
  df["case_when_statement"] = df.apply(create_case_when_string, axis=1)
  type_of_admission_definition = create_categorical_definition_string(df["case_statement"])
  type_of_admission_definitions_separate = create_binary_definitions_string(df["case_when_statement"])
  
  # Using the above definitions, write a full SQL statement to identify them as well as their META_TYPE as either COVID, COVID Excluded (U073 or U074 in non-primary as only covid evidence) or Non-COVID
  query_string = f"""
SELECT
  *,
  -- ROW_NUMBER() OVER (PARTITION BY PERSON_ID_DEID ORDER BY CASE WHEN TYPE_OF_ADMISSION NOT RLIKE 'Exclude' THEN 1 WHEN TYPE_OF_ADMISSION NOT RLIKE 'Other|No Type' THEN 2 ELSE 3 END, ADMIDATE ASC) AS ADMISSION_IDX,
  CASE WHEN TYPE_OF_ADMISSION NOT RLIKE 'Exclude' THEN 'COVID' WHEN TYPE_OF_ADMISSION NOT RLIKE 'Other|No Type' THEN 'COVID Excluded' ELSE 'Non-COVID' END AS META_TYPE
FROM (
  SELECT
    *,
    {type_of_admission_definition},
    {type_of_admission_definitions_separate}
  FROM global_temp.{table_name}
)"""
  print(query_string)
  return query_string