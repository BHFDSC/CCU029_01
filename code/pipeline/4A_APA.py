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
  input_table_name = f"dars_nic_391419_j3w9t_collab.{first_infections_output_table_name}"
  output_table_name = cohort_w_lookback_output_table_name
except:
  raise ValueError("RUN TABLE_NAMES FIRST")

spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_cohort_for_lookback AS
SELECT * FROM {input_table_name}
""")

if verbose:
  print(f"Subsetting each of the three lookback datasets to just the IDs included in the cohort (`{input_table_name}`)...")

# COMMAND ----------

# Subset each of the three lookback datasets to just the IDs included in the cohort

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_hes_apc_subset AS
# MAGIC SELECT a.*
# MAGIC FROM global_temp.ccu029_hes_apc a
# MAGIC INNER JOIN global_temp.ccu029_01_cohort_for_lookback b
# MAGIC ON a.PERSON_ID_DEID = b.PERSON_ID_DEID;
# MAGIC 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_hes_op_subset AS
# MAGIC SELECT a.*
# MAGIC FROM global_temp.ccu029_hes_op a
# MAGIC INNER JOIN global_temp.ccu029_01_cohort_for_lookback b
# MAGIC ON a.PERSON_ID_DEID = b.PERSON_ID_DEID;
# MAGIC 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_gdppr_subset AS
# MAGIC SELECT a.*
# MAGIC FROM global_temp.ccu029_gdppr a
# MAGIC INNER JOIN global_temp.ccu029_01_cohort_for_lookback b
# MAGIC ON a.NHS_NUMBER_DEID = b.PERSON_ID_DEID;

# COMMAND ----------

periods = ["apa", "nine_months", "two_years", "five_years"]

def generate_lookback_table_statement(hes_type, periods):
  tables = [f"ccu029_01_{hes_type}_{period}_lookback" for period in periods]
  col_names = []
  joins = []
  for i, table in enumerate(tables):
    col_names += [col_name for col_name in spark.sql(f"SELECT * FROM global_temp.{table}").schema.names if col_name not in ["PERSON_ID_DEID", "LOOKBACK_DATE"]]
    joins.append(f"LEFT JOIN global_temp.{table} {chr(98 + i)}\nON a.PERSON_ID_DEID = {chr(98 + i)}.PERSON_ID_DEID AND a.LOOKBACK_DATE = {chr(98 + i)}.LOOKBACK_DATE\n")
  col_names_formatted = ",\n  ".join(col_names)
  sql_statement = f"""
SELECT
  a.PERSON_ID_DEID,
  a.LOOKBACK_DATE,
  {col_names_formatted}
FROM global_temp.ccu029_01_cohort_for_lookback a
{''.join(joins)}"""
  return sql_statement, col_names

# COMMAND ----------

if verbose:
  print("Creating consistent lookback tables for HES APC...")

# COMMAND ----------

# Similar to our cohort creation, we are initially concerned with consistency.
# 
# We ensure each admission-person pair has a consistent discharge date, then filter to only valid records i.e. non-NULL admission date and a valid discharge date occurring on or after admission.
# 
# We then group by admission date and concatenate all of the codes we are interested in to then be able to lookback through relative to each admission in our cohort.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_hes_apc_fixed_disdate AS
# MAGIC SELECT
# MAGIC   PERSON_ID_DEID,
# MAGIC   ADMIDATE,
# MAGIC   MAX(DISDATE) AS DISDATE_FIXED
# MAGIC FROM global_temp.ccu029_01_hes_apc_subset
# MAGIC GROUP BY PERSON_ID_DEID, ADMIDATE;
# MAGIC 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_hes_apc_consistent AS
# MAGIC SELECT
# MAGIC   a.PERSON_ID_DEID,
# MAGIC   a.ADMIDATE,
# MAGIC   NULLIF(CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(a.DIAG_4_CONCAT, ','))))), '') AS DAILY_HES_APC_ICD10_CODES,
# MAGIC   NULLIF(CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(NULLIF(a.OPERTN_3_CONCAT, '-'), ','))))), '') AS DAILY_OPERTN_3_CONCAT,
# MAGIC   NULLIF(CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(NULLIF(a.OPERTN_4_CONCAT, '-'), ','))))), '') AS DAILY_OPERTN_4_CONCAT
# MAGIC FROM global_temp.ccu029_01_hes_apc_subset a
# MAGIC INNER JOIN global_temp.ccu029_01_hes_apc_fixed_disdate b
# MAGIC ON a.PERSON_ID_DEID = b.PERSON_ID_DEID AND a.ADMIDATE = b.ADMIDATE
# MAGIC WHERE
# MAGIC   a.PERSON_ID_DEID IS NOT NULL
# MAGIC   AND a.ADMIDATE IS NOT NULL
# MAGIC   AND (DISDATE_FIXED >= a.ADMIDATE OR DISDATE_FIXED IS NULL)
# MAGIC GROUP BY a.PERSON_ID_DEID, a.ADMIDATE;
# MAGIC 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_hes_apc_apa_lookback AS
# MAGIC SELECT
# MAGIC   a.PERSON_ID_DEID,
# MAGIC   a.LOOKBACK_DATE,
# MAGIC   COUNT(*) AS APA_HES_APC_COUNT,
# MAGIC   NULLIF(CONCAT_WS(',', COLLECT_LIST(COALESCE(b.ADMIDATE, ''))), '') AS APA_HES_APC_DATES,
# MAGIC   CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(b.DAILY_HES_APC_ICD10_CODES, ','))))) AS APA_HES_APC_DIAG,
# MAGIC   CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(NULLIF(b.DAILY_OPERTN_3_CONCAT, '-'), ','))))) AS APA_HES_APC_OPERTN_3_CONCAT,
# MAGIC   CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(NULLIF(b.DAILY_OPERTN_4_CONCAT, '-'), ','))))) AS APA_HES_APC_OPERTN_4_CONCAT
# MAGIC FROM global_temp.ccu029_01_cohort_for_lookback a
# MAGIC INNER JOIN (SELECT * FROM global_temp.ccu029_01_hes_apc_consistent DISTRIBUTE BY PERSON_ID_DEID SORT BY PERSON_ID_DEID, ADMIDATE ASC) b
# MAGIC ON a.PERSON_ID_DEID = b.PERSON_ID_DEID AND b.ADMIDATE <= a.LOOKBACK_DATE
# MAGIC GROUP BY a.PERSON_ID_DEID, a.LOOKBACK_DATE;
# MAGIC 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_hes_apc_nine_months_lookback AS
# MAGIC SELECT
# MAGIC   a.PERSON_ID_DEID,
# MAGIC   a.LOOKBACK_DATE,
# MAGIC   COUNT(*) AS NINE_MONTHS_HES_APC_COUNT,
# MAGIC   NULLIF(CONCAT_WS(',', COLLECT_LIST(COALESCE(b.ADMIDATE, ''))), '') AS NINE_MONTHS_HES_APC_DATES,
# MAGIC   CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(b.DAILY_HES_APC_ICD10_CODES, ','))))) AS NINE_MONTHS_HES_APC_DIAG,
# MAGIC   CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(NULLIF(b.DAILY_OPERTN_3_CONCAT, '-'), ','))))) AS NINE_MONTHS_HES_APC_OPERTN_3_CONCAT,
# MAGIC   CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(NULLIF(b.DAILY_OPERTN_4_CONCAT, '-'), ','))))) AS NINE_MONTHS_HES_APC_OPERTN_4_CONCAT
# MAGIC FROM global_temp.ccu029_01_cohort_for_lookback a
# MAGIC INNER JOIN (SELECT * FROM global_temp.ccu029_01_hes_apc_consistent DISTRIBUTE BY PERSON_ID_DEID SORT BY PERSON_ID_DEID, ADMIDATE ASC) b
# MAGIC ON a.PERSON_ID_DEID = b.PERSON_ID_DEID AND b.ADMIDATE BETWEEN ADD_MONTHS(a.LOOKBACK_DATE, -9) AND a.LOOKBACK_DATE
# MAGIC GROUP BY a.PERSON_ID_DEID, a.LOOKBACK_DATE;
# MAGIC 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_hes_apc_two_years_lookback AS
# MAGIC SELECT
# MAGIC   a.PERSON_ID_DEID,
# MAGIC   a.LOOKBACK_DATE,
# MAGIC   COUNT(*) AS TWO_YEARS_HES_APC_COUNT,
# MAGIC   NULLIF(CONCAT_WS(',', COLLECT_LIST(COALESCE(b.ADMIDATE, ''))), '') AS TWO_YEARS_HES_APC_DATES,
# MAGIC   CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(b.DAILY_HES_APC_ICD10_CODES, ','))))) AS TWO_YEARS_HES_APC_DIAG,
# MAGIC   CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(NULLIF(b.DAILY_OPERTN_3_CONCAT, '-'), ','))))) AS TWO_YEARS_HES_APC_OPERTN_3_CONCAT,
# MAGIC   CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(NULLIF(b.DAILY_OPERTN_4_CONCAT, '-'), ','))))) AS TWO_YEARS_HES_APC_OPERTN_4_CONCAT
# MAGIC FROM global_temp.ccu029_01_cohort_for_lookback a
# MAGIC INNER JOIN (SELECT * FROM global_temp.ccu029_01_hes_apc_consistent DISTRIBUTE BY PERSON_ID_DEID SORT BY PERSON_ID_DEID, ADMIDATE ASC) b
# MAGIC ON a.PERSON_ID_DEID = b.PERSON_ID_DEID AND b.ADMIDATE BETWEEN ADD_MONTHS(a.LOOKBACK_DATE, -24) AND a.LOOKBACK_DATE
# MAGIC GROUP BY a.PERSON_ID_DEID, a.LOOKBACK_DATE;
# MAGIC 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_hes_apc_five_years_lookback AS
# MAGIC SELECT
# MAGIC   a.PERSON_ID_DEID,
# MAGIC   a.LOOKBACK_DATE,
# MAGIC   COUNT(*) AS FIVE_YEARS_HES_APC_COUNT,
# MAGIC   NULLIF(CONCAT_WS(',', COLLECT_LIST(COALESCE(b.ADMIDATE, ''))), '') AS FIVE_YEARS_HES_APC_DATES,
# MAGIC   CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(b.DAILY_HES_APC_ICD10_CODES, ','))))) AS FIVE_YEARS_HES_APC_DIAG,
# MAGIC   CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(NULLIF(b.DAILY_OPERTN_3_CONCAT, '-'), ','))))) AS FIVE_YEARS_HES_APC_OPERTN_3_CONCAT,
# MAGIC   CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(NULLIF(b.DAILY_OPERTN_4_CONCAT, '-'), ','))))) AS FIVE_YEARS_HES_APC_OPERTN_4_CONCAT
# MAGIC FROM global_temp.ccu029_01_cohort_for_lookback a
# MAGIC INNER JOIN (SELECT * FROM global_temp.ccu029_01_hes_apc_consistent DISTRIBUTE BY PERSON_ID_DEID SORT BY PERSON_ID_DEID, ADMIDATE ASC) b
# MAGIC ON a.PERSON_ID_DEID = b.PERSON_ID_DEID AND b.ADMIDATE BETWEEN ADD_MONTHS(a.LOOKBACK_DATE, -60) AND a.LOOKBACK_DATE
# MAGIC GROUP BY a.PERSON_ID_DEID, a.LOOKBACK_DATE

# COMMAND ----------

hes_apc_statement, hes_apc_cols = generate_lookback_table_statement("hes_apc", periods)

hes_apc_lookback = spark.sql(hes_apc_statement)

if verbose:
  print(f"Creating `{cohort_hes_apc_lookback_table_name}` with study start date == {study_start}")

hes_apc_lookback.createOrReplaceGlobalTempView(cohort_hes_apc_lookback_table_name)
drop_table(cohort_hes_apc_lookback_table_name)
create_table(cohort_hes_apc_lookback_table_name)
optimise_table(cohort_hes_apc_lookback_table_name, "PERSON_ID_DEID")
create_temp_table(cohort_hes_apc_lookback_table_name, "cohort_hes_apc_lookback_table_name")

table = spark.sql(f"SELECT * FROM dars_nic_391419_j3w9t_collab.{cohort_hes_apc_lookback_table_name}")
print(f"`{cohort_hes_apc_lookback_table_name}` has {table.count()} rows and {len(table.columns)} columns.")

# COMMAND ----------

if verbose:
  print("Creating consistent lookback tables for HES OP...")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_hes_op_consistent AS
# MAGIC SELECT
# MAGIC   a.PERSON_ID_DEID,
# MAGIC   a.APPTDATE,
# MAGIC   NULLIF(CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(a.DIAG_4_CONCAT, ','))))), '') AS DAILY_HES_OP_ICD10_CODES
# MAGIC FROM global_temp.ccu029_01_hes_op_subset a
# MAGIC INNER JOIN global_temp.ccu029_01_hes_op_subset b
# MAGIC ON a.PERSON_ID_DEID = b.PERSON_ID_DEID
# MAGIC WHERE
# MAGIC   a.PERSON_ID_DEID IS NOT NULL
# MAGIC   AND a.APPTDATE IS NOT NULL
# MAGIC GROUP BY a.PERSON_ID_DEID, a.APPTDATE;
# MAGIC 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_hes_op_apa_lookback AS
# MAGIC SELECT
# MAGIC   a.PERSON_ID_DEID,
# MAGIC   a.LOOKBACK_DATE,
# MAGIC   COUNT(*) AS APA_HES_OP_COUNT,
# MAGIC   NULLIF(CONCAT_WS(',', COLLECT_LIST(COALESCE(b.APPTDATE, ''))), '') AS APA_HES_OP_DATES,
# MAGIC   CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(b.DAILY_HES_OP_ICD10_CODES, ','))))) AS APA_HES_OP_DIAG
# MAGIC FROM global_temp.ccu029_01_cohort_for_lookback a
# MAGIC INNER JOIN (SELECT * FROM global_temp.ccu029_01_hes_op_consistent DISTRIBUTE BY PERSON_ID_DEID SORT BY PERSON_ID_DEID, APPTDATE ASC) b
# MAGIC ON a.PERSON_ID_DEID = b.PERSON_ID_DEID AND b.APPTDATE <= a.LOOKBACK_DATE
# MAGIC GROUP BY a.PERSON_ID_DEID, a.LOOKBACK_DATE;
# MAGIC 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_hes_op_nine_months_lookback AS
# MAGIC SELECT
# MAGIC   a.PERSON_ID_DEID,
# MAGIC   a.LOOKBACK_DATE,
# MAGIC   COUNT(*) AS NINE_MONTHS_HES_OP_COUNT,
# MAGIC   NULLIF(CONCAT_WS(',', COLLECT_LIST(COALESCE(b.APPTDATE, ''))), '') AS NINE_MONTHS_HES_OP_DATES,
# MAGIC   CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(b.DAILY_HES_OP_ICD10_CODES, ','))))) AS NINE_MONTHS_HES_OP_DIAG
# MAGIC FROM global_temp.ccu029_01_cohort_for_lookback a
# MAGIC INNER JOIN (SELECT * FROM global_temp.ccu029_01_hes_op_consistent DISTRIBUTE BY PERSON_ID_DEID SORT BY PERSON_ID_DEID, APPTDATE ASC) b
# MAGIC ON a.PERSON_ID_DEID = b.PERSON_ID_DEID AND b.APPTDATE BETWEEN ADD_MONTHS(a.LOOKBACK_DATE, -9) AND a.LOOKBACK_DATE
# MAGIC GROUP BY a.PERSON_ID_DEID, a.LOOKBACK_DATE;
# MAGIC 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_hes_op_two_years_lookback AS
# MAGIC SELECT
# MAGIC   a.PERSON_ID_DEID,
# MAGIC   a.LOOKBACK_DATE,
# MAGIC   COUNT(*) AS TWO_YEARS_HES_OP_COUNT,
# MAGIC   NULLIF(CONCAT_WS(',', COLLECT_LIST(COALESCE(b.APPTDATE, ''))), '') AS TWO_YEARS_HES_OP_DATES,
# MAGIC   CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(b.DAILY_HES_OP_ICD10_CODES, ','))))) AS TWO_YEARS_HES_OP_DIAG
# MAGIC FROM global_temp.ccu029_01_cohort_for_lookback a
# MAGIC INNER JOIN (SELECT * FROM global_temp.ccu029_01_hes_op_consistent DISTRIBUTE BY PERSON_ID_DEID SORT BY PERSON_ID_DEID, APPTDATE ASC) b
# MAGIC ON a.PERSON_ID_DEID = b.PERSON_ID_DEID AND b.APPTDATE BETWEEN ADD_MONTHS(a.LOOKBACK_DATE, -24) AND a.LOOKBACK_DATE
# MAGIC GROUP BY a.PERSON_ID_DEID, a.LOOKBACK_DATE;
# MAGIC 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_hes_op_five_years_lookback AS
# MAGIC SELECT
# MAGIC   a.PERSON_ID_DEID,
# MAGIC   a.LOOKBACK_DATE,
# MAGIC   COUNT(*) AS FIVE_YEARS_HES_OP_COUNT,
# MAGIC   NULLIF(CONCAT_WS(',', COLLECT_LIST(COALESCE(b.APPTDATE, ''))), '') AS FIVE_YEARS_HES_OP_DATES,
# MAGIC   CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(b.DAILY_HES_OP_ICD10_CODES, ','))))) AS FIVE_YEARS_HES_OP_DIAG
# MAGIC FROM global_temp.ccu029_01_cohort_for_lookback a
# MAGIC INNER JOIN (SELECT * FROM global_temp.ccu029_01_hes_op_consistent DISTRIBUTE BY PERSON_ID_DEID SORT BY PERSON_ID_DEID, APPTDATE ASC) b
# MAGIC ON a.PERSON_ID_DEID = b.PERSON_ID_DEID AND b.APPTDATE BETWEEN ADD_MONTHS(a.LOOKBACK_DATE, -60) AND a.LOOKBACK_DATE
# MAGIC GROUP BY a.PERSON_ID_DEID, a.LOOKBACK_DATE

# COMMAND ----------

hes_op_statement, hes_op_cols = generate_lookback_table_statement("hes_op", periods)

hes_op_lookback = spark.sql(hes_op_statement)

if verbose:
  print(f"Creating `{cohort_hes_op_lookback_table_name}` with study start date == {study_start}")

hes_op_lookback.createOrReplaceGlobalTempView(cohort_hes_op_lookback_table_name)
drop_table(cohort_hes_op_lookback_table_name)
create_table(cohort_hes_op_lookback_table_name)
optimise_table(cohort_hes_op_lookback_table_name, "PERSON_ID_DEID")
create_temp_table(cohort_hes_op_lookback_table_name, "cohort_hes_op_lookback_table_name")

table = spark.sql(f"SELECT * FROM dars_nic_391419_j3w9t_collab.{cohort_hes_op_lookback_table_name}")
print(f"`{cohort_hes_op_lookback_table_name}` has {table.count()} rows and {len(table.columns)} columns.")

# COMMAND ----------

if verbose:
  print("Creating consistent lookback tables for GDPPR...")

# COMMAND ----------

# GDPPR is a special case as it is coded under SNOMED rather than ICD-10, so we introduce an intermediary step that converts the SNOMED codes to ICD-10 via a TRUD mapping

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_gdppr_consistent AS
# MAGIC SELECT
# MAGIC   gp.NHS_NUMBER_DEID,
# MAGIC   gp.DATE,
# MAGIC   NULLIF(CONCAT_WS(',', ARRAY_DISTINCT(COLLECT_LIST(trud.ICD))), '') AS DAILY_GDPPR_ICD10_CODES
# MAGIC FROM global_temp.ccu029_01_gdppr_subset gp
# MAGIC INNER JOIN (
# MAGIC   SELECT
# MAGIC     REFERENCED_COMPONENT_ID, -- SNOMED-CT code
# MAGIC     MAP_TARGET as ICD
# MAGIC   FROM dss_corporate.snomed_ct_rf2_map_to_icd10_v01
# MAGIC   WHERE substring(MAP_TARGET,1,1) != "#" AND ICD10_VERSION = "5th Ed"
# MAGIC ) trud
# MAGIC ON gp.CODE = trud.REFERENCED_COMPONENT_ID
# MAGIC WHERE
# MAGIC   gp.NHS_NUMBER_DEID IS NOT NULL
# MAGIC   AND gp.DATE IS NOT NULL
# MAGIC GROUP BY gp.NHS_NUMBER_DEID, gp.DATE;
# MAGIC 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_gdppr_apa_lookback AS
# MAGIC SELECT
# MAGIC   a.PERSON_ID_DEID,
# MAGIC   a.LOOKBACK_DATE,
# MAGIC   COUNT(*) AS APA_GDPPR_COUNT,
# MAGIC   NULLIF(CONCAT_WS(',', COLLECT_LIST(COALESCE(b.DATE, ''))), '') AS APA_GDPPR_DATES,
# MAGIC   CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(b.DAILY_GDPPR_ICD10_CODES, ','))))) AS APA_GDPPR_DIAG
# MAGIC FROM global_temp.ccu029_01_cohort_for_lookback a
# MAGIC INNER JOIN (SELECT * FROM global_temp.ccu029_01_gdppr_consistent DISTRIBUTE BY NHS_NUMBER_DEID SORT BY NHS_NUMBER_DEID, DATE ASC) b
# MAGIC ON a.PERSON_ID_DEID = b.NHS_NUMBER_DEID AND b.DATE <= a.LOOKBACK_DATE
# MAGIC GROUP BY a.PERSON_ID_DEID, a.LOOKBACK_DATE;
# MAGIC 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_gdppr_nine_months_lookback AS
# MAGIC SELECT
# MAGIC   a.PERSON_ID_DEID,
# MAGIC   a.LOOKBACK_DATE,
# MAGIC   COUNT(*) AS NINE_MONTHS_GDPPR_COUNT,
# MAGIC   NULLIF(CONCAT_WS(',', COLLECT_LIST(COALESCE(b.DATE, ''))), '') AS NINE_MONTHS_GDPPR_DATES,
# MAGIC   CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(b.DAILY_GDPPR_ICD10_CODES, ','))))) AS NINE_MONTHS_GDPPR_DIAG
# MAGIC FROM global_temp.ccu029_01_cohort_for_lookback a
# MAGIC INNER JOIN (SELECT * FROM global_temp.ccu029_01_gdppr_consistent DISTRIBUTE BY NHS_NUMBER_DEID SORT BY NHS_NUMBER_DEID, DATE ASC) b
# MAGIC ON a.PERSON_ID_DEID = b.NHS_NUMBER_DEID AND b.DATE BETWEEN ADD_MONTHS(a.LOOKBACK_DATE, -9) AND a.LOOKBACK_DATE
# MAGIC GROUP BY a.PERSON_ID_DEID, a.LOOKBACK_DATE;
# MAGIC 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_gdppr_two_years_lookback AS
# MAGIC SELECT
# MAGIC   a.PERSON_ID_DEID,
# MAGIC   a.LOOKBACK_DATE,
# MAGIC   COUNT(*) AS TWO_YEARS_GDPPR_COUNT,
# MAGIC   NULLIF(CONCAT_WS(',', COLLECT_LIST(COALESCE(b.DATE, ''))), '') AS TWO_YEARS_GDPPR_DATES,
# MAGIC   CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(b.DAILY_GDPPR_ICD10_CODES, ','))))) AS TWO_YEARS_GDPPR_DIAG
# MAGIC FROM global_temp.ccu029_01_cohort_for_lookback a
# MAGIC INNER JOIN (SELECT * FROM global_temp.ccu029_01_gdppr_consistent DISTRIBUTE BY NHS_NUMBER_DEID SORT BY NHS_NUMBER_DEID, DATE ASC) b
# MAGIC ON a.PERSON_ID_DEID = b.NHS_NUMBER_DEID AND b.DATE BETWEEN ADD_MONTHS(a.LOOKBACK_DATE, -24) AND a.LOOKBACK_DATE
# MAGIC GROUP BY a.PERSON_ID_DEID, a.LOOKBACK_DATE;
# MAGIC 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_gdppr_five_years_lookback AS
# MAGIC SELECT
# MAGIC   a.PERSON_ID_DEID,
# MAGIC   a.LOOKBACK_DATE,
# MAGIC   COUNT(*) AS FIVE_YEARS_GDPPR_COUNT,
# MAGIC   NULLIF(CONCAT_WS(',', COLLECT_LIST(COALESCE(b.DATE, ''))), '') AS FIVE_YEARS_GDPPR_DATES,
# MAGIC   CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(b.DAILY_GDPPR_ICD10_CODES, ','))))) AS FIVE_YEARS_GDPPR_DIAG
# MAGIC FROM global_temp.ccu029_01_cohort_for_lookback a
# MAGIC INNER JOIN (SELECT * FROM global_temp.ccu029_01_gdppr_consistent DISTRIBUTE BY NHS_NUMBER_DEID SORT BY NHS_NUMBER_DEID, DATE ASC) b
# MAGIC ON a.PERSON_ID_DEID = b.NHS_NUMBER_DEID AND b.DATE BETWEEN ADD_MONTHS(a.LOOKBACK_DATE, -60) AND a.LOOKBACK_DATE
# MAGIC GROUP BY a.PERSON_ID_DEID, a.LOOKBACK_DATE

# COMMAND ----------

gdppr_statement, gdppr_cols = generate_lookback_table_statement("gdppr", periods)

gdppr_lookback = spark.sql(gdppr_statement)

if verbose:
  print(f"Creating `{cohort_gdppr_lookback_table_name}` with study start date == {study_start}")

gdppr_lookback.createOrReplaceGlobalTempView(cohort_gdppr_lookback_table_name)
drop_table(cohort_gdppr_lookback_table_name)
create_table(cohort_gdppr_lookback_table_name)
optimise_table(cohort_gdppr_lookback_table_name, "PERSON_ID_DEID")
create_temp_table(cohort_gdppr_lookback_table_name, "cohort_gdppr_lookback_table_name")

table = spark.sql(f"SELECT * FROM dars_nic_391419_j3w9t_collab.{cohort_gdppr_lookback_table_name}")
print(f"`{cohort_gdppr_lookback_table_name}` has {table.count()} rows and {len(table.columns)} columns.")

# COMMAND ----------

if verbose:
  print("Joining all of the lookback tables to the main cohort...")

# COMMAND ----------

def create_concatenated_cols(periods):
  periods = [period.upper() for period in periods]
  cols = []
  for period in periods:
    cols.append(" + ".join([f"COALESCE({period}_{source}_COUNT, 0)" for source in ["HES_APC", "HES_OP", "GDPPR"]]) + f" AS {period}_COUNT")
    cols.append(" + ".join([f"COALESCE({period}_{source}_COUNT, 0)" for source in ["HES_APC", "HES_OP"]]) + f" AS {period}_COUNT_NO_GDPPR")
    cols.append("CONCAT_WS(',', " + ", ".join([f"NULLIF({period}_{source}_DIAG, '')" for source in ["HES_APC", "HES_OP", "GDPPR"]]) + f") AS {period}_DIAG")
    cols.append("CONCAT_WS(',', " + ", ".join([f"NULLIF({period}_{source}_DIAG, '')" for source in ["HES_APC", "HES_OP"]]) + f") AS {period}_DIAG_NO_GDPPR")
  return cols

all_cols = ",\n  ".join(hes_apc_cols + hes_op_cols + gdppr_cols + create_concatenated_cols(periods))

# COMMAND ----------

lookback = spark.sql(f"""
SELECT
  a.*,
  {all_cols}
FROM global_temp.ccu029_01_cohort_for_lookback a
INNER JOIN global_temp.ccu029_01_cohort_hes_apc_lookback_table_name b
ON a.PERSON_ID_DEID = b.PERSON_ID_DEID AND a.LOOKBACK_DATE = b.LOOKBACK_DATE
INNER JOIN global_temp.ccu029_01_cohort_hes_op_lookback_table_name c
ON a.PERSON_ID_DEID = c.PERSON_ID_DEID AND a.LOOKBACK_DATE = c.LOOKBACK_DATE
INNER JOIN global_temp.ccu029_01_cohort_gdppr_lookback_table_name d
ON a.PERSON_ID_DEID = d.PERSON_ID_DEID AND a.LOOKBACK_DATE = d.LOOKBACK_DATE""")

if verbose:
  print(f"Creating `{output_table_name}` with study start date == {study_start}")

lookback.createOrReplaceGlobalTempView(output_table_name)
drop_table(output_table_name)
create_table(output_table_name)
optimise_table(output_table_name, "PERSON_ID_DEID")

table = spark.sql(f"SELECT * FROM dars_nic_391419_j3w9t_collab.{output_table_name}")
print(f"`{output_table_name}` has {table.count()} rows and {len(table.columns)} columns.")

# COMMAND ----------

if test:
  display(spark.sql(f"SELECT COUNT(*) FROM dars_nic_391419_j3w9t_collab.{output_table_name} WHERE APA_DIAG NOT RLIKE 'U071|U072|U073|U074'"))

# COMMAND ----------

if test:
  display(spark.sql(f"SELECT APA_DIAG, APA_HES_APC_DIAG, APA_HES_OP_DIAG, APA_GDPPR_DIAG, APA_COUNT, APA_HES_APC_COUNT, APA_HES_OP_COUNT, APA_GDPPR_COUNT, APA_HES_APC_DATES, * FROM dars_nic_391419_j3w9t_collab.{output_table_name}"))

# COMMAND ----------

if test:
  print("Checking no rows were lost by adding lookback info...")
  n = spark.sql(f"SELECT COUNT(*) FROM global_temp.ccu029_01_cohort_for_lookback").first()[0]
  n_ad_look = spark.sql(f"SELECT COUNT(*) FROM dars_nic_391419_j3w9t_collab.{output_table_name}").first()[0]
  print(n, n_ad_look)
  assert n == n_ad_look

# COMMAND ----------

