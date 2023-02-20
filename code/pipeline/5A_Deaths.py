# Databricks notebook source
# MAGIC %run ../config/quiet

# COMMAND ----------

# MAGIC %run ../auxiliary/helper_functions

# COMMAND ----------

# MAGIC %run ./TABLE_NAMES

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
  input_table_name = f"dars_nic_391419_j3w9t_collab.{cohort_w_all_uhcs_output_table_name}"
  output_table_name = cohort_w_deaths_output_table_name
except:
  raise ValueError("RUN TABLE_NAMES FIRST")

spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_cohort_for_deaths AS
SELECT * FROM {input_table_name}
""")

if verbose:
  print("Running tests on deaths data...")

# COMMAND ----------

# # Deaths

# According to the documentation, here `DEC_CONF_NHS_NUMBER_CLEAN_DEID` is the patient identifier column, there are a large number of duplicate IDs in the table. 

# The following few queries illustrate some data quality issues, eventually we just aggregate at a patient ID level and return concatenated COD in primary and non-primary positions for all records that are then joined via this ID to our cohort.

# COMMAND ----------

if test:
  display(spark.sql(f"""
SELECT
  COUNT(*) as N,
  COUNT(DISTINCT DEC_CONF_NHS_NUMBER_CLEAN_DEID) as COUNT_DISTINCT_ID,
  COUNT(*) - COUNT(DISTINCT DEC_CONF_NHS_NUMBER_CLEAN_DEID) as COUNT_DUPLICATE_ID
FROM
  global_temp.ccu029_deaths
WHERE
  -- Needed for QC
  to_date(REG_DATE_OF_DEATH,'yyyyMMdd') <= '{study_end}'"""))

# COMMAND ----------

if test:
  display(spark.sql(f"""
SELECT COUNT(DISTINCT PERSON_ID_DEID), COUNT(*) FROM (
  SELECT
    DEC_CONF_NHS_NUMBER_CLEAN_DEID as PERSON_ID_DEID,
    MIN(to_date(REG_DATE_OF_DEATH, 'yyyyMMdd')) as REG_DATE_OF_DEATH
  FROM global_temp.ccu029_deaths
  WHERE
    DEC_CONF_NHS_NUMBER_CLEAN_DEID IS NOT NULL
    AND to_date(REG_DATE_OF_DEATH, 'yyyyMMdd') <= '{study_end}'
  GROUP BY PERSON_ID_DEID
)"""))

# COMMAND ----------

if test:
  display(spark.sql(f"""
SELECT SUM(N) - COUNT(*) AS NUMBER_OF_REPEAT_PATIENT_RECORDS_ON_SAME_DOD
FROM (
  SELECT
    a.DEC_CONF_NHS_NUMBER_CLEAN_DEID,
    COUNT(a.DEC_CONF_NHS_NUMBER_CLEAN_DEID) AS N
  FROM global_temp.ccu029_deaths a
  INNER JOIN (
    SELECT
      DEC_CONF_NHS_NUMBER_CLEAN_DEID,
      MIN(to_date(REG_DATE_OF_DEATH, 'yyyyMMdd')) as REG_DATE_OF_DEATH
    FROM global_temp.ccu029_deaths
    WHERE
      DEC_CONF_NHS_NUMBER_CLEAN_DEID IS NOT NULL
      AND to_date(REG_DATE_OF_DEATH,'yyyyMMdd') <= '{study_end}'
    GROUP BY DEC_CONF_NHS_NUMBER_CLEAN_DEID
  ) b
  ON 
    a.DEC_CONF_NHS_NUMBER_CLEAN_DEID = b.DEC_CONF_NHS_NUMBER_CLEAN_DEID
    AND to_date(a.REG_DATE_OF_DEATH, 'yyyyMMdd') = b.REG_DATE_OF_DEATH
  GROUP BY a.DEC_CONF_NHS_NUMBER_CLEAN_DEID
  HAVING COUNT(a.DEC_CONF_NHS_NUMBER_CLEAN_DEID) > 1
)"""))

# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_deaths_to_join AS
SELECT
  1 AS DEATH,
  DEC_CONF_NHS_NUMBER_CLEAN_DEID AS PERSON_ID_DEID,
  MIN(to_date(REG_DATE_OF_DEATH, 'yyyyMMdd')) AS REG_DATE_OF_DEATH,
  CONCAT_WS(',', COLLECT_LIST(to_date(REG_DATE_OF_DEATH, 'yyyyMMdd'))) AS ALL_REG_DATE_OF_DEATH_debug,
  COUNT(*) AS RECORD_COUNT,
  NULLIF(CONCAT_WS(',', FLATTEN(COLLECT_LIST(SPLIT(NULLIF(CONCAT_WS(',', S_COD_CODE_1, S_COD_CODE_2, S_COD_CODE_3, S_COD_CODE_4, S_COD_CODE_5, S_COD_CODE_6, S_COD_CODE_7, S_COD_CODE_8, S_COD_CODE_9, S_COD_CODE_10, S_COD_CODE_11, S_COD_CODE_12, S_COD_CODE_13, S_COD_CODE_14, S_COD_CODE_15), ''), ',')))), '') AS COD_CODES,
  NULLIF(CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(NULLIF(CONCAT_WS(',', S_COD_CODE_1, S_COD_CODE_2, S_COD_CODE_3, S_COD_CODE_4, S_COD_CODE_5, S_COD_CODE_6, S_COD_CODE_7, S_COD_CODE_8, S_COD_CODE_9, S_COD_CODE_10, S_COD_CODE_11, S_COD_CODE_12, S_COD_CODE_13, S_COD_CODE_14, S_COD_CODE_15), ''), ','))))), '') AS UNIQUE_COD_CODES,
  NULLIF(CONCAT_WS(',', FLATTEN(COLLECT_LIST(SPLIT(NULLIF(CONCAT_WS(',', ARRAY_EXCEPT(ARRAY(S_COD_CODE_1, S_COD_CODE_2, S_COD_CODE_3, S_COD_CODE_4, S_COD_CODE_5, S_COD_CODE_6, S_COD_CODE_7, S_COD_CODE_8, S_COD_CODE_9, S_COD_CODE_10, S_COD_CODE_11, S_COD_CODE_12, S_COD_CODE_13, S_COD_CODE_14, S_COD_CODE_15), ARRAY(S_UNDERLYING_COD_ICD10))), ''), ',')))), '') AS COD_CODES_SECONDARY,
  NULLIF(CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(NULLIF(CONCAT_WS(',', ARRAY_EXCEPT(ARRAY(S_COD_CODE_1, S_COD_CODE_2, S_COD_CODE_3, S_COD_CODE_4, S_COD_CODE_5, S_COD_CODE_6, S_COD_CODE_7, S_COD_CODE_8, S_COD_CODE_9, S_COD_CODE_10, S_COD_CODE_11, S_COD_CODE_12, S_COD_CODE_13, S_COD_CODE_14, S_COD_CODE_15), ARRAY(S_UNDERLYING_COD_ICD10))), ''), ','))))), '') AS UNIQUE_COD_CODES_SECONDARY,
  NULLIF(CONCAT_WS(',', COLLECT_LIST(NULLIF(S_UNDERLYING_COD_ICD10, ''))), '') AS COD_CODES_PRIMARY,
  NULLIF(CONCAT_WS(',', ARRAY_DISTINCT(COLLECT_LIST(NULLIF(S_UNDERLYING_COD_ICD10, '')))), '') AS UNIQUE_COD_CODES_PRIMARY
FROM global_temp.ccu029_deaths
WHERE
  DEC_CONF_NHS_NUMBER_CLEAN_DEID IS NOT NULL
  AND to_date(REG_DATE_OF_DEATH, 'yyyyMMdd') <= '{study_end}'
GROUP BY PERSON_ID_DEID""")

if verbose:
  print("Joining dates and causes of death to the cohort...")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_cohort_w_deaths AS
# MAGIC SELECT
# MAGIC   a.*,
# MAGIC   COALESCE(DEATH, 0) as DEATH,
# MAGIC   REG_DATE_OF_DEATH,
# MAGIC   ALL_REG_DATE_OF_DEATH_debug,
# MAGIC   CASE WHEN REG_DATE_OF_DEATH BETWEEN a.ADMIDATE AND a.DISDATE THEN 1 ELSE 0 END AS DIED_IN_HOSPITAL,
# MAGIC   RECORD_COUNT,
# MAGIC   CASE WHEN COD_CODES_PRIMARY RLIKE 'U071|U072|U073|U074' THEN 1 ELSE 0 END AS COVID_PRIMARY_COD, 
# MAGIC   CASE WHEN COD_CODES_SECONDARY RLIKE 'U071|U072|U073|U074' THEN 1 ELSE 0 END AS COVID_SECONDARY_COD,
# MAGIC   CASE WHEN COD_CODES_PRIMARY RLIKE 'U075|U109|M303|R65' AND COD_CODES NOT RLIKE "A01|A02|A03|A04|A05|A37|A38|A39|A40|A41|B95" THEN 1 ELSE 0 END AS PIMS_PRIMARY_COD,
# MAGIC   CASE WHEN COD_CODES_SECONDARY RLIKE 'U075|U109|M303|R65' AND COD_CODES NOT RLIKE "A01|A02|A03|A04|A05|A37|A38|A39|A40|A41|B95" THEN 1 ELSE 0 END AS PIMS_SECONDARY_COD,
# MAGIC   COD_CODES,
# MAGIC   UNIQUE_COD_CODES,
# MAGIC   COD_CODES_SECONDARY,
# MAGIC   UNIQUE_COD_CODES_SECONDARY,
# MAGIC   COD_CODES_PRIMARY,
# MAGIC   UNIQUE_COD_CODES_PRIMARY
# MAGIC FROM global_temp.ccu029_01_cohort_for_deaths a
# MAGIC LEFT JOIN global_temp.ccu029_01_deaths_to_join b
# MAGIC ON a.PERSON_ID_DEID = b.PERSON_ID_DEID

# COMMAND ----------

cohort_joined = spark.sql("SELECT * FROM global_temp.ccu029_01_cohort_w_deaths")

if verbose:
  print(f"Creating `{output_table_name}` with study start date == {study_start}")

cohort_joined.createOrReplaceGlobalTempView(output_table_name)
drop_table(output_table_name)
create_table(output_table_name)

optimise_table(output_table_name, "PERSON_ID_DEID")

table = spark.sql(f"SELECT * FROM dars_nic_391419_j3w9t_collab.{output_table_name}")
print(f"`{output_table_name}` has {table.count()} rows and {len(table.columns)} columns.")

# COMMAND ----------

if test:
  display(spark.sql(f"SELECT COUNT(*) FROM dars_nic_391419_j3w9t_collab.{output_table_name}"))