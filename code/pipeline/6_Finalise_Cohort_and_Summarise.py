# Databricks notebook source
# MAGIC %run ../config/quiet

# COMMAND ----------

# MAGIC %run ../auxiliary/helper_functions

# COMMAND ----------

# MAGIC %run ./TABLE_NAMES

# COMMAND ----------

# MAGIC %run ../auxiliary/ethnicity_and_sex

# COMMAND ----------

try:
  test
except:
  test = True
  
try:
  verbose
except:
  verbose = False

# COMMAND ----------

try:
  input_table_name = f"dars_nic_391419_j3w9t_collab.{cohort_w_vaccinations_output_table_name}"
  output_table_name = final_cohort_output_table_name
except:
  raise ValueError("RUN TABLE_NAMES FIRST")

spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_cohort_for_finishing AS
SELECT * FROM {input_table_name}
""")

if verbose:
  print("Adding final variables to the cohort...")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_cohort_final AS
# MAGIC SELECT
# MAGIC   *,
# MAGIC   SEX AS SEX_NUMERIC,
# MAGIC   CASE WHEN DEATH = 1 AND (COVID_PRIMARY_COD = 1 OR COVID_SECONDARY_COD = 1) THEN 1 ELSE 0 END AS COVID_RELATED_DEATH,
# MAGIC   CASE WHEN DEATH = 1 AND (PIMS_PRIMARY_COD = 1 OR PIMS_SECONDARY_COD = 1) THEN 1 ELSE 0 END AS PIMS_RELATED_DEATH,
# MAGIC   CASE WHEN DEATH = 1 AND (COVID_PRIMARY_COD = 1 OR PIMS_PRIMARY_COD = 1) THEN 1 ELSE 0 END AS COVID_OR_PIMS_UNDERLYING_DEATH,
# MAGIC   CASE WHEN DEATH = 1 AND (COVID_PRIMARY_COD = 1 OR COVID_SECONDARY_COD = 1 OR PIMS_PRIMARY_COD = 1 OR PIMS_SECONDARY_COD = 1) THEN 1 ELSE 0 END AS COVID_OR_PIMS_RELATED_DEATH,
# MAGIC   CASE WHEN DIED_IN_HOSPITAL = 1 AND (COVID_PRIMARY_COD = 1 OR PIMS_PRIMARY_COD = 1) THEN 1 ELSE 0 END AS COVID_OR_PIMS_UNDERLYING_DEATH_IN_HOSPITAL,
# MAGIC   CASE WHEN DIED_IN_HOSPITAL = 1 AND (COVID_PRIMARY_COD = 1 OR COVID_SECONDARY_COD = 1 OR PIMS_PRIMARY_COD = 1 OR PIMS_SECONDARY_COD = 1) THEN 1 ELSE 0 END AS COVID_OR_PIMS_RELATED_DEATH_IN_HOSPITAL,
# MAGIC   CASE WHEN POSITIVE_COVID_TEST_IN_WINDOW = 1 OR PREVIOUS_INFECTION = 1 THEN 1 ELSE 0 END AS COVID_POSITIVE_IN_WINDOW_OR_PRIOR_TO_ADMISSION,
# MAGIC   CASE WHEN GREEN_BOOK_RISK_FACTOR == 1 OR MD_RISK_FACTOR == 1 THEN 1 ELSE 0 END AS ANY_RISK_FACTOR,
# MAGIC   CASE
# MAGIC     WHEN INFECTION_DATE <= '2020-12-05' THEN "Original"
# MAGIC     WHEN INFECTION_DATE <= '2021-01-02' THEN "Inter-Original-Alpha"
# MAGIC     WHEN INFECTION_DATE <= '2021-05-01' THEN "Alpha"
# MAGIC     WHEN INFECTION_DATE <= '2021-05-29' THEN "Inter-Alpha-Delta"
# MAGIC     WHEN INFECTION_DATE <= '2021-12-11' THEN "Delta"
# MAGIC     WHEN INFECTION_DATE <= '2021-12-25' THEN "Inter-Delta-Omicron"
# MAGIC     ELSE "Omicron"
# MAGIC   END AS INFECTION_VARIANT_PERIOD,
# MAGIC   CASE
# MAGIC     WHEN DIAG_4_CONCAT_PRIMARY RLIKE "U071" THEN "U071"
# MAGIC     WHEN DIAG_4_CONCAT_PRIMARY RLIKE "U072" THEN "U072"
# MAGIC     WHEN DIAG_4_CONCAT_PRIMARY RLIKE "U073" THEN "U073"
# MAGIC     WHEN DIAG_4_CONCAT_PRIMARY RLIKE "U074" THEN "U074"
# MAGIC     ELSE "None"
# MAGIC   END AS PRIMRY_DIAG_COVID_CODE,
# MAGIC   CASE
# MAGIC     WHEN DIAG_4_CONCAT_SECONDARY RLIKE "U071" THEN "U071"
# MAGIC     WHEN DIAG_4_CONCAT_SECONDARY RLIKE "U072" THEN "U072"
# MAGIC     WHEN DIAG_4_CONCAT_SECONDARY RLIKE "U073" THEN "U073"
# MAGIC     WHEN DIAG_4_CONCAT_SECONDARY RLIKE "U074" THEN "U074"
# MAGIC     ELSE "None"
# MAGIC   END AS SECONDARY_DIAG_COVID_CODE,
# MAGIC   CASE
# MAGIC     WHEN DIAG_4_CONCAT RLIKE "U071" THEN "U071"
# MAGIC     WHEN DIAG_4_CONCAT RLIKE "U072" THEN "U072"
# MAGIC     WHEN DIAG_4_CONCAT RLIKE "U073" THEN "U073"
# MAGIC     WHEN DIAG_4_CONCAT RLIKE "U074" THEN "U074"
# MAGIC     ELSE "None"
# MAGIC   END AS DIAG_COVID_CODE,
# MAGIC   CASE
# MAGIC     WHEN AGE < 1 THEN "< 1"
# MAGIC     WHEN AGE < 5 THEN "1 - 4"
# MAGIC     WHEN AGE < 12 THEN "5 - 11"
# MAGIC     WHEN AGE < 16 THEN "12 - 15"
# MAGIC     WHEN AGE < 18 THEN "16 - 17"
# MAGIC     ELSE "ERROR"
# MAGIC   END AS AGE_CAT,
# MAGIC   CASE
# MAGIC     WHEN AGE BETWEEN 2 AND 11.99999 THEN "2 - 11 (Age 2 - School Year 6)"
# MAGIC     WHEN AGE BETWEEN 12 AND 16.99999 THEN "12 - 16 (School Year 7 - Year 11)"
# MAGIC     ELSE "Remainder"
# MAGIC   END AS ONS_AGE_GROUP,
# MAGIC   CASE
# MAGIC     WHEN POSITIVE_COVID_TEST_IN_UKHSA_WINDOW == 1 THEN "UKHSA Type 1"
# MAGIC     WHEN COVID_PRIMARY_HOSPITALISATION == 1 THEN "UKHSA Type 3"
# MAGIC     WHEN COVID_SECONDARY_HOSPITALISATION == 1 THEN "(Possible) UKHSA Type 2"
# MAGIC     ELSE "(Possible) UKHSA Type 4"
# MAGIC   END AS UKHSA_LABEL,
# MAGIC   CASE WHEN ETHNIC == "" THEN "9" ELSE ETHNIC END AS ETHNIC_CLEAN
# MAGIC FROM global_temp.ccu029_01_cohort_for_finishing

# COMMAND ----------

cohort = spark.sql("SELECT * FROM global_temp.ccu029_01_cohort_final")

# Map
cohort_cleaned = cohort \
  .withColumn("ETHNIC_GROUP", mapping_expr_ethnic_group[col("ETHNIC_CLEAN")]) \
  .withColumn("ETHNICITY", mapping_expr_ethnicity[col("ETHNIC_CLEAN")]) \
  .drop("ETHNIC_CLEAN") \
  .drop("ETHNIC") \
  .withColumn("SEX", mapping_expr_sex[col("SEX")])

if verbose:
  print(f"Creating `{output_table_name}` with study start date == {study_start}")

cohort_cleaned.createOrReplaceGlobalTempView(output_table_name)
drop_table(output_table_name)
create_table(output_table_name)

optimise_table(output_table_name, 'PERSON_ID_DEID')

table = spark.sql(f"SELECT * FROM dars_nic_391419_j3w9t_collab.{output_table_name}")
print(f"`{output_table_name}` has {table.count()} rows and {len(table.columns)} columns.")

# COMMAND ----------

if test:
  uhc_names_1 = ',\n  '.join(list(dict.fromkeys([f"SUM(CASE WHEN UHC == '{x.replace('PRESENT_CODES_GREEN_BOOK_UHC_', '').replace('PRESENT_CODES_FIVE_YEARS_GREEN_BOOK_UHC_', '')}' THEN 1 ELSE 0 END) AS N_{x.replace('PRESENT_CODES_GREEN_BOOK_UHC_', '').replace('PRESENT_CODES_FIVE_YEARS_GREEN_BOOK_UHC_', '')}" for x in table.schema.names if "PRESENT_CODES_GREEN_BOOK" in x or "PRESENT_CODES_FIVE_YEARS_GREEN_BOOK" in x])))
  print(uhc_names_1)
  uhc_names_2 = '\n  UNION ALL\n  '.join([f"SELECT EXPLODE(ARRAY_DISTINCT(SPLIT({x}, ','))) AS ICD10_CODE, '{x.replace('PRESENT_CODES_GREEN_BOOK_UHC_', '').replace('PRESENT_CODES_FIVE_YEARS_GREEN_BOOK_UHC_', '')}' AS UHC\n  FROM dars_nic_391419_j3w9t_collab.{output_table_name}" for x in table.schema.names if "PRESENT_CODES_GREEN_BOOK" in x or "PRESENT_CODES_FIVE_YEARS_GREEN_BOOK" in x])
  print(uhc_names_2)
  
  separate_UHC_code_counts = spark.sql(f"""
SELECT
  {uhc_names_1},
  ICD10_CODE,
  ICD10_DESCRIPTION,
  DESCRIPTIONS_ABBREVIATED,
  ICD10_CHAPTER_HEADING,
  ICD10_CHAPTER_DESCRIPTION,
  ICD10_GROUP_HEADING,
  ICD10_GROUP_DESCRIPTION
FROM (
  {uhc_names_2}
)
INNER JOIN dss_corporate.icd10_group_chapter_v01
ON ICD10_CODE = ALT_CODE
GROUP BY ICD10_CODE, ICD10_DESCRIPTION, DESCRIPTIONS_ABBREVIATED, ICD10_CHAPTER_HEADING, ICD10_CHAPTER_DESCRIPTION, ICD10_GROUP_HEADING, ICD10_GROUP_DESCRIPTION
ORDER BY ICD10_CODE""")
  
  table_name = "ccu029_01_separate_green_book_uhc_code_counts"
  
  if verbose:
    print(f"Creating `{table_name}` with study start date == {study_start}")

  separate_UHC_code_counts.createOrReplaceGlobalTempView(table_name)
  drop_table(table_name)
  create_table(table_name)

# COMMAND ----------

if test:
  uhc_names_1 = ',\n  '.join(list(dict.fromkeys([f"SUM(CASE WHEN UHC == '{x.replace('PRESENT_CODES_MD_UHC_', '').replace('PRESENT_CODES_FIVE_YEARS_MD_UHC_', '')}' THEN 1 ELSE 0 END) AS N_{x.replace('PRESENT_CODES_MD_UHC_', '').replace('PRESENT_CODES_FIVE_YEARS_MD_UHC_', '')}" for x in table.schema.names if "PRESENT_CODES_MD" in x or "PRESENT_CODES_FIVE_YEARS_MD" in x])))
  print(uhc_names_1)
  uhc_names_2 = '\n  UNION ALL\n  '.join([f"SELECT EXPLODE(ARRAY_DISTINCT(SPLIT({x}, ','))) AS ICD10_CODE, '{x.replace('PRESENT_CODES_MD_UHC_', '').replace('PRESENT_CODES_FIVE_YEARS_MD_UHC_', '')}' AS UHC\n  FROM dars_nic_391419_j3w9t_collab.{output_table_name}" for x in table.schema.names if "PRESENT_CODES_MD" in x or "PRESENT_CODES_FIVE_YEARS_MD" in x])
  print(uhc_names_2)
  
  separate_UHC_code_counts = spark.sql(f"""
SELECT
  {uhc_names_1},
  ICD10_CODE,
  ICD10_DESCRIPTION,
  DESCRIPTIONS_ABBREVIATED,
  ICD10_CHAPTER_HEADING,
  ICD10_CHAPTER_DESCRIPTION,
  ICD10_GROUP_HEADING,
  ICD10_GROUP_DESCRIPTION
FROM (
  {uhc_names_2}
)
INNER JOIN dss_corporate.icd10_group_chapter_v01
ON ICD10_CODE = ALT_CODE
GROUP BY ICD10_CODE, ICD10_DESCRIPTION, DESCRIPTIONS_ABBREVIATED, ICD10_CHAPTER_HEADING, ICD10_CHAPTER_DESCRIPTION, ICD10_GROUP_HEADING, ICD10_GROUP_DESCRIPTION
ORDER BY ICD10_CODE""")
  
  table_name = "ccu029_01_separate_md_uhc_code_counts"
  
  if verbose:
    print(f"Creating `{table_name}` with study start date == {study_start}")

  separate_UHC_code_counts.createOrReplaceGlobalTempView(table_name)
  drop_table(table_name)
  create_table(table_name)

# COMMAND ----------

if test:
  code_col_names = ", ".join([f"SPLIT({x}, ',')" for x in table.schema.names if "PRESENT_CODES_GREEN_BOOK" in x])
  print(code_col_names)

# COMMAND ----------

if test:
  PIMS_UHC_code_counts = spark.sql(f"""
SELECT COUNT(*) AS N_OCCURRENCES, ICD10_CODE, ICD10_DESCRIPTION, DESCRIPTIONS_ABBREVIATED, ICD10_CHAPTER_HEADING, ICD10_CHAPTER_DESCRIPTION, ICD10_GROUP_HEADING, ICD10_GROUP_DESCRIPTION FROM (
  SELECT EXPLODE(FLATTEN(COLLECT_LIST(FLATTEN(ARRAY_DISTINCT(ARRAY({code_col_names})))))) AS ICD10_CODE
  FROM dars_nic_391419_j3w9t_collab.{output_table_name}
  WHERE GREEN_BOOK_RISK_FACTOR = 1 AND TYPE_OF_ADMISSION = 'PIMS'
)
INNER JOIN dss_corporate.icd10_group_chapter_v01
ON ICD10_CODE = ALT_CODE
GROUP BY ICD10_CODE, ICD10_DESCRIPTION, DESCRIPTIONS_ABBREVIATED, ICD10_CHAPTER_HEADING, ICD10_CHAPTER_DESCRIPTION, ICD10_GROUP_HEADING, ICD10_GROUP_DESCRIPTION
ORDER BY N_OCCURRENCES DESC""")
  
  table_name = "ccu029_01_pims_uhc_code_counts"
  
  if verbose:
    print(f"Creating `{table_name}` with study start date == {study_start}")

  PIMS_UHC_code_counts.createOrReplaceGlobalTempView(table_name)
  drop_table(table_name)
  create_table(table_name)

  table = spark.sql(f"SELECT * FROM dars_nic_391419_j3w9t_collab.{table_name} ORDER BY N_OCCURRENCES DESC")
  print(f"`{table_name}` has {table.count()} rows and {len(table.columns)} columns.")
  
  display(table)

# COMMAND ----------

if test:
  PIMS_UHC_code_counts = spark.sql(f"""
SELECT COUNT(*) AS N_OCCURRENCES, ICD10_CODE, ICD10_DESCRIPTION, DESCRIPTIONS_ABBREVIATED, ICD10_CHAPTER_HEADING, ICD10_CHAPTER_DESCRIPTION, ICD10_GROUP_HEADING, ICD10_GROUP_DESCRIPTION FROM (
  SELECT EXPLODE(FLATTEN(COLLECT_LIST(FLATTEN(ARRAY_DISTINCT(ARRAY({code_col_names})))))) AS ICD10_CODE
  FROM dars_nic_391419_j3w9t_collab.{output_table_name}
  WHERE GREEN_BOOK_RISK_FACTOR = 1
)
INNER JOIN dss_corporate.icd10_group_chapter_v01
ON ICD10_CODE = ALT_CODE
GROUP BY ICD10_CODE, ICD10_DESCRIPTION, DESCRIPTIONS_ABBREVIATED, ICD10_CHAPTER_HEADING, ICD10_CHAPTER_DESCRIPTION, ICD10_GROUP_HEADING, ICD10_GROUP_DESCRIPTION
ORDER BY N_OCCURRENCES DESC""")
  
  table_name = "ccu029_01_uhc_code_counts"
  
  if verbose:
    print(f"Creating `{table_name}` with study start date == {study_start}")

  PIMS_UHC_code_counts.createOrReplaceGlobalTempView(table_name)
  drop_table(table_name)
  create_table(table_name)

  table = spark.sql(f"SELECT * FROM dars_nic_391419_j3w9t_collab.{table_name} ORDER BY N_OCCURRENCES DESC")
  print(f"`{table_name}` has {table.count()} rows and {len(table.columns)} columns.")
  
  display(table)

# COMMAND ----------

