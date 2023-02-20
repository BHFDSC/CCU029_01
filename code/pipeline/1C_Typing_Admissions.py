# Databricks notebook source
# MAGIC %run ../config/quiet

# COMMAND ----------

# MAGIC %run ../auxiliary/helper_functions

# COMMAND ----------

# MAGIC %run ./TABLE_NAMES

# COMMAND ----------

# MAGIC %run ../auxiliary/admission_type_functions

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
  input_table_name = f"dars_nic_391419_j3w9t_collab.{admissions_w_icu_output_table_name}"
  output_table_name = admissions_typed_output_table_name
except:
  raise ValueError("RUN TABLE_NAMES FIRST")

spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_admissions_for_typing AS
SELECT * FROM {input_table_name}
""")

if verbose:
  print("Applying the hierachy of type definitions to all admissions...")

# COMMAND ----------

query_string = create_typed_admissions_query("ccu029_01_admissions_for_typing")

all_typed_admissions = spark.sql(query_string)

if verbose:
  print(f"Creating `{output_table_name}` with study start date == {study_start}")

all_typed_admissions.createOrReplaceGlobalTempView(output_table_name)
drop_table(output_table_name)
create_table(output_table_name)

optimise_table(output_table_name, "PERSON_ID_DEID")

table = spark.sql(f"SELECT * FROM dars_nic_391419_j3w9t_collab.{output_table_name}")
print(f"`{output_table_name}` has {table.count()} rows and {len(table.columns)} columns.")

# COMMAND ----------

display(spark.sql(f"SELECT TYPE_OF_ADMISSION, META_TYPE, COUNT(*) FROM dars_nic_391419_j3w9t_collab.{output_table_name} GROUP BY TYPE_OF_ADMISSION, META_TYPE ORDER BY META_TYPE, TYPE_OF_ADMISSION"))

# COMMAND ----------

display(spark.sql(f"SELECT TYPE_OF_ADMISSION, META_TYPE, COUNT(*) FROM dars_nic_391419_j3w9t_collab.{output_table_name} WHERE (PIMS_REQUIREMENT == 1 OR COVID_PRIMARY_HOSPITALISATION == 1 OR COVID_DEFINITE_SECONDARY_HOSPITALISATION == 1 OR POSITIVE_COVID_TEST_INCLUSION == 1) GROUP BY TYPE_OF_ADMISSION, META_TYPE ORDER BY META_TYPE, TYPE_OF_ADMISSION"))

# COMMAND ----------

display(spark.sql(f"SELECT 100 * SUM(CASE WHEN META_TYPE == 'COVID Missed' THEN 1 ELSE 0 END) / COUNT(*) AS PERCENT_MISSED, 100 * SUM(CASE WHEN META_TYPE == 'COVID Excluded' THEN 1 ELSE 0 END) / COUNT(*) AS PERCENT_EXCLUDED FROM dars_nic_391419_j3w9t_collab.{output_table_name} WHERE (PIMS_REQUIREMENT == 1 OR COVID_PRIMARY_HOSPITALISATION == 1 OR COVID_DEFINITE_SECONDARY_HOSPITALISATION == 1 OR POSITIVE_COVID_TEST_INCLUSION == 1)"))

# COMMAND ----------

if test:
  spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_impossible_admissions AS
SELECT a.PERSON_ID_DEID, a.ADMIDATE
FROM dars_nic_391419_j3w9t_collab.{output_table_name} a
INNER JOIN dars_nic_391419_j3w9t_collab.{output_table_name} b
ON a.PERSON_ID_DEID = b.PERSON_ID_DEID AND a.ADMIDATE < b.ADMIDATE
WHERE a.DISDATE IS NULL""")

# COMMAND ----------

if test:
  display(spark.sql(f"""
SELECT
  TYPE_OF_ADMISSION, META_TYPE, COUNT(*)
FROM (
  SELECT x.*
  FROM (
    SELECT *
    FROM dars_nic_391419_j3w9t_collab.{output_table_name}
    WHERE ADMISSION_AGE < 18 AND TYPE_OF_ADMISSION NOT RLIKE 'Other|No Type'
  ) x
  ANTI JOIN global_temp.ccu029_01_impossible_admissions y
  ON x.PERSON_ID_DEID = y.PERSON_ID_DEID AND x.ADMIDATE = y.ADMIDATE
)
WHERE ADMIDATE >= '{study_start}'
GROUP BY TYPE_OF_ADMISSION, META_TYPE
ORDER BY META_TYPE, TYPE_OF_ADMISSION"""))

# COMMAND ----------

if test:
  display(spark.sql(f"""
SELECT
  CASE WHEN DISDATE IS NULL THEN 1 ELSE 0 END, COUNT(*)
FROM (
  SELECT x.*
  FROM (
    SELECT *
    FROM dars_nic_391419_j3w9t_collab.{output_table_name}
    WHERE ADMISSION_AGE < 18 AND TYPE_OF_ADMISSION NOT RLIKE 'Other|No Type'
  ) x
  ANTI JOIN global_temp.ccu029_01_impossible_admissions y
  ON x.PERSON_ID_DEID = y.PERSON_ID_DEID AND x.ADMIDATE = y.ADMIDATE
)
WHERE ADMIDATE >= '{study_start}'
GROUP BY CASE WHEN DISDATE IS NULL THEN 1 ELSE 0 END"""))

# COMMAND ----------

if test:
  display(spark.sql(f"SELECT LENGTH_OF_STAY, COUNT(*) FROM dars_nic_391419_j3w9t_collab.{output_table_name} WHERE TYPE_OF_ADMISSION == 'Nosocomial' GROUP BY LENGTH_OF_STAY ORDER BY LENGTH_OF_STAY"))

# COMMAND ----------

