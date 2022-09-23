# Databricks notebook source
# We read in UHC definitions and generate the required code to identify these amongst a set of medical records via historical presence of ICD-10 codes

# COMMAND ----------

# MAGIC %run ./UHC_definitions

# COMMAND ----------

import io
import pandas as pd
from functools import reduce
from operator import add
from pyspark.sql.functions import lit, col, when


# Converts a standard comma separated list string into regex union, formatting requirements safely allow for incomplete code stems to be used, i.e. "C1" for all codes starting with those two characters
def list_to_regex(input_string, sep=","):
  return f"'{'|'.join(['^' + x for x in input_string.split(sep)] + [',' + x for x in input_string.split(sep)])}'"


# Programmatically builds the required SQL snippets to identify a UHC via rules extracted from the include + exclude codes as well as its type and name
def create_uhc_definition_statements(name, includes, includes_5_years, excludes, excludes_5_years, uhc_type, sep=","):
  filter_list = []
  filter_5_years_list = []
  # Write code requirements using the definitions imported above
  if includes:
    filter_list.append(f"ALT_CODE RLIKE {list_to_regex(includes, sep)}")
  if excludes:
    filter_list.append(f"ALT_CODE NOT RLIKE {list_to_regex(excludes, sep)}")
  if includes_5_years:
    filter_5_years_list.append(f"ALT_CODE RLIKE {list_to_regex(includes_5_years, sep)}")
  if excludes_5_years:
    filter_5_years_list.append(f"ALT_CODE NOT RLIKE {list_to_regex(excludes_5_years, sep)}")
  filter_string = " AND ".join(filter_list)
  filter_5_years_string = " AND ".join(filter_5_years_list)
  
  
  if name == "OBESITY":
    # Obesity is a special case for Green Book and MD UHCs where different conditions are required based on BMI and weight-for-age / bmi-for-age Z scores that change depending on the age of the person, as well as presence of obesity codes as usual
    lookback_list = "TWO_YEARS_DIAG"
    if uhc_type == "GREEN_BOOK_UHC":
      case_option = f"AGE >= 16 AND (BMI > 40 OR BMI_Z_SCORE > 3 OR WEIGHT_Z_SCORE > 3 OR SIZE(ARRAY_INTERSECT(POSSIBLE_CODES_{uhc_type}_{name}, SPLIT({lookback_list}, ','))) > 0)"
    else:
      case_option = f"BMI > 30 OR (AGE < 5 AND (BMI_Z_SCORE > 3 OR WEIGHT_Z_SCORE > 3)) OR (AGE >= 5 AND (BMI_Z_SCORE > 2 OR WEIGHT_Z_SCORE > 2)) OR SIZE(ARRAY_INTERSECT(POSSIBLE_CODES_{uhc_type}_{name}, SPLIT({lookback_list}, ','))) > 0"
  elif name == "PREGNANCY":
    # Pregnancy is another special case, when we are dealing with a record where a COVID admission is present, use the diagnostic codes associated with the record, otherwise look back 9 months
    lookback_list = "CASE WHEN COVID_ADMISSION_IN_WINDOW == 1 THEN DIAG_4_CONCAT ELSE NINE_MONTHS_DIAG END"
    case_option = f"AGE > 5 AND SIZE(ARRAY_INTERSECT(POSSIBLE_CODES_{uhc_type}_{name}, SPLIT({lookback_list}, ','))) > 0"
  else:
    # There can be one or both of the 5 year or standard APA filter strings present, when both are, either being satisifed is sufficient
    lookback_list = "APA_DIAG"
    case_option_list = []
    if filter_string:
      case_option_list.append(f"SIZE(ARRAY_INTERSECT(POSSIBLE_CODES_{uhc_type}_{name}, SPLIT(APA_DIAG, ','))) > 0")
    if filter_5_years_string:
      case_option_list.append(f"SIZE(ARRAY_INTERSECT(POSSIBLE_CODES_FIVE_YEARS_{uhc_type}_{name}, SPLIT(FIVE_YEARS_DIAG, ','))) > 0")
    case_option = " OR ".join(case_option_list)

  # This list will contain the snippet(s) that collects a list of all of the ICD-10 codes from the full list of ICD-10 codes that are associated with a UHC
  possible_codes_outputs = []
  # This list will first contain a snippet that uses the `case_option` string to identify the UHC amongst the possible codes identified in the statements stored in the list above, and also the snippet(s) that collect the actual codes represent in each record that are the reason for its presence amongst those identified with the UHC
  definitions_outputs = [f"\n  CASE WHEN {case_option} THEN 1 ELSE 0 END AS {uhc_type}_{name}"]
  if filter_string:
    definitions_outputs.append(f"\n  CONCAT_WS(',', ARRAY_INTERSECT(POSSIBLE_CODES_{uhc_type}_{name}, SPLIT({lookback_list}, ','))) AS PRESENT_CODES_{uhc_type}_{name}")
    possible_codes_outputs.append(f"\n    COLLECT_LIST(CASE WHEN {filter_string} THEN ALT_CODE ELSE NULL END) AS POSSIBLE_CODES_{uhc_type}_{name}")
  if filter_5_years_string:
    definitions_outputs.append(f"\n  CONCAT_WS(',', ARRAY_INTERSECT(POSSIBLE_CODES_FIVE_YEARS_{uhc_type}_{name}, SPLIT(FIVE_YEARS_DIAG, ','))) AS PRESENT_CODES_FIVE_YEARS_{uhc_type}_{name}")
    possible_codes_outputs.append(f"\n    COLLECT_LIST(CASE WHEN {filter_5_years_string} THEN ALT_CODE ELSE NULL END) AS POSSIBLE_CODES_FIVE_YEARS_{uhc_type}_{name}")
  
  return [",".join(definitions_outputs), ",".join(possible_codes_outputs)]


def generate_full_query(df, input_table, uhc_type, sep=","):
  query_frags = [create_uhc_definition_statements(*row, uhc_type, sep) for row in zip(df['name'], df['includes'], df['includes_5_years'], df['excludes'], df['excludes_5_years'])]
  # The CROSS JOIN part of this adds the single row table created within the sub-select statement to each row of the cohort, in order to then use those possible codes to identify UHCs in the cohort
  query = f"""
SELECT
  a.*,{','.join([q[0] for q in query_frags])}
FROM {input_table} a
CROSS JOIN (
  SELECT{','.join([q[1] for q in query_frags])}
  FROM dss_corporate.icd10_group_chapter_v01
) b"""
  if verbose:
    print(query)
  return query


def row_sum_across(*cols):
    return reduce(add, cols, lit(0))


def formulate_uhcs(uhc_type, input_table):
  df = pd.read_csv(io.StringIO(UHC[uhc_type])).fillna(False)
  df["name"] = df["name"].str.replace(" ", "_").str.upper()
  cohort = spark.sql(generate_full_query(df, input_table, f"{uhc_type}_UHC"))

  uhcs = df["name"]
  vars = [col(f"{uhc_type}_UHC_" + x) for x in uhcs]

  # Add columns for the multimorbidity and binary flags for the presence of any UHC or risk factor
  cohort = cohort.withColumn(f"{uhc_type}_MULTIMORBIDITY", row_sum_across(*vars))
  cohort = cohort.withColumn(f"{uhc_type}_RISK_FACTOR", when(cohort[f"{uhc_type}_MULTIMORBIDITY"] >= 1, 1).otherwise(0))
  cohort = cohort.withColumn(f"{uhc_type}_UHC", when(cohort[f"{uhc_type}_MULTIMORBIDITY"] - (cohort[f"{uhc_type}_UHC_OBESITY"] + cohort[f"{uhc_type}_UHC_PREGNANCY"]) >= 1, 1).otherwise(0))

  return cohort