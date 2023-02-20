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
  input_table_name = f"dars_nic_391419_j3w9t_collab.{cohort_w_lookback_output_table_name}"
  output_table_name = cohort_w_bmi_output_table_name
except:
  raise ValueError("RUN TABLE_NAMES FIRST")

spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_cohort_for_bmi AS
SELECT * FROM {input_table_name}
""")

if verbose:
  print("Subsetting GDPPR to only the records that fall within each person in the cohort's admission date and 2 years prior to it...")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_gdppr_subset AS
# MAGIC SELECT a.*
# MAGIC FROM global_temp.ccu029_gdppr a
# MAGIC INNER JOIN global_temp.ccu029_01_cohort_for_bmi b
# MAGIC ON a.NHS_NUMBER_DEID = b.PERSON_ID_DEID AND a.DATE BETWEEN ADD_MONTHS(b.LOOKBACK_DATE, -24) AND b.LOOKBACK_DATE

# COMMAND ----------

# Lists are from OpenSAFELY, we will use these to identify weight and height data from the GDPPR subset defined above
# 
# https://www.opencodelists.org/codelist/opensafely/weight-snomed/5459abc6/#full-list
# 
# https://www.opencodelists.org/codelist/opensafely/height-snomed/3b4a3891/#full-list

# COMMAND ----------

import io
import pandas as pd

if verbose:
  print(f"Generating `{cohort_codelist_weight_table_name}` and `{cohort_codelist_height_table_name}` to identify weight and height records in GDPPR...")

weight_snomed = """
code,term
139985004,O/E - weight
162763007,On examination - weight
248341004,General weight finding
248345008,Body weight
27113001,Body weight
271604008,Weight finding
301333006,Finding of measures of body weight
363808001,Measured body weight
424927000,Body weight with shoes
425024002,Body weight without shoes
735395000,Current body weight
784399000,Self reported body weight
"""

df = pd.read_csv(io.StringIO(weight_snomed))
df = spark.createDataFrame(df)
df.createOrReplaceGlobalTempView(cohort_codelist_weight_table_name)
drop_table(cohort_codelist_weight_table_name)
create_table(cohort_codelist_weight_table_name)
create_temp_table(cohort_codelist_weight_table_name, "cohort_codelist_weight_table_name")

height_snomed = """
code,term
139977008,O/E - height
14456009,Measuring height of patient
162755006,On examination - height
248327008,General finding of height
248333004,Standing height
50373000,Body height measure
"""

df = pd.read_csv(io.StringIO(height_snomed))
df = spark.createDataFrame(df)
df.createOrReplaceGlobalTempView(cohort_codelist_height_table_name)
drop_table(cohort_codelist_height_table_name)
create_table(cohort_codelist_height_table_name)
create_temp_table(cohort_codelist_height_table_name, "cohort_codelist_height_table_name")


# COMMAND ----------

if verbose:
  print("Creating weight and height tables for the cohort...")

# COMMAND ----------

# Apply transformations to weights and heights data to try and standardise units, cut off erroneous records etc.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_all_weights AS
# MAGIC SELECT
# MAGIC   NHS_NUMBER_DEID,
# MAGIC   DATE AS WEIGHT_DATE,
# MAGIC   VALUE1_CONDITION AS WEIGHT_DIRTY,
# MAGIC   CASE WHEN VALUE1_CONDITION > 400 THEN VALUE1_CONDITION / 1000 ELSE VALUE1_CONDITION END AS WEIGHT,
# MAGIC   CASE WHEN (CASE WHEN VALUE1_CONDITION > 400 THEN VALUE1_CONDITION / 1000 ELSE VALUE1_CONDITION END) BETWEEN 0.2 AND 400 THEN 1 ELSE 0 END AS ACCEPTABLE_WEIGHT,
# MAGIC   gp.CODE AS WEIGHT_CODE
# MAGIC FROM global_temp.ccu029_01_gdppr_subset gp
# MAGIC INNER JOIN global_temp.ccu029_01_cohort_codelist_weight_table_name lkp_wt
# MAGIC ON gp.CODE = lkp_wt.code
# MAGIC WHERE VALUE1_CONDITION IS NOT NULL

# COMMAND ----------

cohort_weights = spark.sql("SELECT * FROM global_temp.ccu029_01_all_weights WHERE ACCEPTABLE_WEIGHT == 1")

if verbose:
  print(f"Creating `{cohort_all_weights_table_name}` with study start date == {study_start}")

cohort_weights.createOrReplaceGlobalTempView(cohort_all_weights_table_name)
drop_table(cohort_all_weights_table_name)
create_table(cohort_all_weights_table_name)
create_temp_table(cohort_all_weights_table_name, "cohort_all_weights_table_name")

table = spark.sql(f"SELECT * FROM dars_nic_391419_j3w9t_collab.{cohort_all_weights_table_name}")
print(f"`{cohort_all_weights_table_name}` has {table.count()} rows and {len(table.columns)} columns.")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_all_heights AS
# MAGIC SELECT
# MAGIC   NHS_NUMBER_DEID,
# MAGIC   DATE AS HEIGHT_DATE,
# MAGIC   VALUE1_CONDITION AS HEIGHT_DIRTY,
# MAGIC   CASE WHEN VALUE1_CONDITION > 2.5 THEN VALUE1_CONDITION / 100 ELSE VALUE1_CONDITION END AS HEIGHT,
# MAGIC   CASE WHEN (CASE WHEN VALUE1_CONDITION > 2.5 THEN VALUE1_CONDITION / 100 ELSE VALUE1_CONDITION END) BETWEEN 0.25 AND 2.5 THEN 1 ELSE 0 END AS ACCEPTABLE_HEIGHT,
# MAGIC   gp.CODE AS HEIGHT_CODE
# MAGIC FROM global_temp.ccu029_01_gdppr_subset gp
# MAGIC INNER JOIN global_temp.ccu029_01_cohort_codelist_height_table_name lkp_ht
# MAGIC ON gp.CODE = lkp_ht.code
# MAGIC WHERE VALUE1_CONDITION IS NOT NULL

# COMMAND ----------

cohort_heights = spark.sql("SELECT * FROM global_temp.ccu029_01_all_heights WHERE ACCEPTABLE_HEIGHT == 1")

if verbose:
  print(f"Creating `{cohort_all_heights_table_name}` with study start date == {study_start}")

cohort_heights.createOrReplaceGlobalTempView(cohort_all_heights_table_name)
drop_table(cohort_all_heights_table_name)
create_table(cohort_all_heights_table_name)
create_temp_table(cohort_all_heights_table_name, "cohort_all_heights_table_name")

table = spark.sql(f"SELECT * FROM dars_nic_391419_j3w9t_collab.{cohort_all_heights_table_name}")
print(f"`{cohort_all_heights_table_name}` has {table.count()} rows and {len(table.columns)} columns.")

# COMMAND ----------

if verbose:
  print("Finding minimum time period between records for individuals with both height and weight data; calculating BMI using this data...")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_all_weights_and_heights AS
# MAGIC SELECT
# MAGIC   COALESCE(a.NHS_NUMBER_DEID, b.NHS_NUMBER_DEID) AS NHS_NUMBER_DEID,
# MAGIC   ABS(DATEDIFF(WEIGHT_DATE, HEIGHT_DATE)) AS WEIGHT_HEIGHT_DATE_DELTA,
# MAGIC   WEIGHT_DATE,
# MAGIC   WEIGHT,
# MAGIC   WEIGHT_CODE,
# MAGIC   HEIGHT_DATE,
# MAGIC   HEIGHT,
# MAGIC   HEIGHT_CODE
# MAGIC FROM global_temp.ccu029_01_cohort_all_weights_table_name a
# MAGIC FULL OUTER JOIN global_temp.ccu029_01_cohort_all_heights_table_name b
# MAGIC ON a.NHS_NUMBER_DEID = b.NHS_NUMBER_DEID;
# MAGIC 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_min_weight_height_deltas AS
# MAGIC SELECT
# MAGIC   NHS_NUMBER_DEID,
# MAGIC   MIN(WEIGHT_HEIGHT_DATE_DELTA) AS MIN_WEIGHT_HEIGHT_DATE_DELTA
# MAGIC FROM global_temp.ccu029_01_all_weights_and_heights
# MAGIC GROUP BY NHS_NUMBER_DEID;
# MAGIC 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_max_weight_height_dates AS
# MAGIC SELECT
# MAGIC   a.NHS_NUMBER_DEID,
# MAGIC   CASE WHEN MAX(WEIGHT_DATE) >= MAX(HEIGHT_DATE) THEN MAX(WEIGHT_DATE) ELSE MAX(HEIGHT_DATE) END AS MAX_DATE,
# MAGIC   CASE WHEN MAX(WEIGHT_DATE) >= MAX(HEIGHT_DATE) THEN "WEIGHT" ELSE "HEIGHT" END AS MAX_DATE_TYPE
# MAGIC FROM global_temp.ccu029_01_all_weights_and_heights a
# MAGIC INNER JOIN global_temp.ccu029_01_min_weight_height_deltas b
# MAGIC ON a.NHS_NUMBER_DEID = b.NHS_NUMBER_DEID AND a.WEIGHT_HEIGHT_DATE_DELTA = b.MIN_WEIGHT_HEIGHT_DATE_DELTA
# MAGIC GROUP BY a.NHS_NUMBER_DEID;
# MAGIC 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_best_weight_and_height AS
# MAGIC SELECT
# MAGIC   a.NHS_NUMBER_DEID,
# MAGIC   a.WEIGHT_HEIGHT_DATE_DELTA,
# MAGIC   AVG(WEIGHT) / (AVG(HEIGHT) * AVG(HEIGHT)) AS BMI,
# MAGIC   a.WEIGHT_DATE,
# MAGIC   a.HEIGHT_DATE,
# MAGIC   AVG(WEIGHT) AS WEIGHT,
# MAGIC   AVG(HEIGHT) AS HEIGHT
# MAGIC FROM global_temp.ccu029_01_all_weights_and_heights a
# MAGIC INNER JOIN global_temp.ccu029_01_min_weight_height_deltas b
# MAGIC ON a.NHS_NUMBER_DEID = b.NHS_NUMBER_DEID AND a.WEIGHT_HEIGHT_DATE_DELTA = b.MIN_WEIGHT_HEIGHT_DATE_DELTA
# MAGIC INNER JOIN global_temp.ccu029_01_max_weight_height_dates c
# MAGIC ON a.NHS_NUMBER_DEID = c.NHS_NUMBER_DEID AND ((MAX_DATE_TYPE = "WEIGHT" AND WEIGHT_DATE = MAX_DATE) OR (MAX_DATE_TYPE = "HEIGHT" AND HEIGHT_DATE = MAX_DATE))
# MAGIC GROUP BY a.NHS_NUMBER_DEID, a.WEIGHT_HEIGHT_DATE_DELTA, a.WEIGHT_DATE, a.HEIGHT_DATE

# COMMAND ----------

cohort_w_weight_and_height = spark.sql("SELECT * FROM global_temp.ccu029_01_best_weight_and_height")

if verbose:
  print(f"Creating `{cohort_best_weight_and_height_table_name}` with study start date == {study_start}")

cohort_w_weight_and_height.createOrReplaceGlobalTempView(cohort_best_weight_and_height_table_name)
drop_table(cohort_best_weight_and_height_table_name)
create_table(cohort_best_weight_and_height_table_name)
create_temp_table(cohort_best_weight_and_height_table_name, "cohort_best_weight_and_height_table_name")

table = spark.sql(f"SELECT * FROM dars_nic_391419_j3w9t_collab.{cohort_best_weight_and_height_table_name}")
print(f"`{cohort_best_weight_and_height_table_name}` has {table.count()} rows and {len(table.columns)} columns.")

# COMMAND ----------

if verbose:
  print("For those indivudals without both weight and height, finding the most recent weight / height...")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_best_weight_max_dates AS
# MAGIC SELECT a.NHS_NUMBER_DEID, MAX(a.WEIGHT_DATE) AS MAX_WEIGHT_DATE
# MAGIC FROM global_temp.ccu029_01_cohort_all_weights_table_name a
# MAGIC -- ANTI JOIN dars_nic_391419_j3w9t_collab.ccu029_01_wip_hosp_cohort_best_weight_and_height b
# MAGIC ANTI JOIN global_temp.ccu029_01_cohort_best_weight_and_height_table_name b
# MAGIC ON a.NHS_NUMBER_DEID = b.NHS_NUMBER_DEID
# MAGIC GROUP BY a.NHS_NUMBER_DEID;
# MAGIC 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_best_height_max_dates AS
# MAGIC SELECT a.NHS_NUMBER_DEID, MAX(a.HEIGHT_DATE) AS MAX_HEIGHT_DATE
# MAGIC FROM global_temp.ccu029_01_cohort_all_heights_table_name a
# MAGIC -- ANTI JOIN dars_nic_391419_j3w9t_collab.ccu029_01_wip_hosp_cohort_best_weight_and_height b
# MAGIC ANTI JOIN global_temp.ccu029_01_cohort_best_weight_and_height_table_name b
# MAGIC ON a.NHS_NUMBER_DEID = b.NHS_NUMBER_DEID
# MAGIC GROUP BY a.NHS_NUMBER_DEID;
# MAGIC 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_best_weight AS
# MAGIC SELECT
# MAGIC   a.NHS_NUMBER_DEID,
# MAGIC   a.WEIGHT_DATE,
# MAGIC   AVG(WEIGHT) AS WEIGHT
# MAGIC FROM global_temp.ccu029_01_cohort_all_weights_table_name a
# MAGIC INNER JOIN global_temp.ccu029_01_best_weight_max_dates b
# MAGIC ON a.NHS_NUMBER_DEID = b.NHS_NUMBER_DEID AND a.WEIGHT_DATE = b.MAX_WEIGHT_DATE
# MAGIC GROUP BY a.NHS_NUMBER_DEID, a.WEIGHT_DATE;
# MAGIC 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_best_height AS
# MAGIC SELECT
# MAGIC   a.NHS_NUMBER_DEID,
# MAGIC   a.HEIGHT_DATE,
# MAGIC   AVG(HEIGHT) AS HEIGHT
# MAGIC FROM global_temp.ccu029_01_cohort_all_heights_table_name a
# MAGIC INNER JOIN global_temp.ccu029_01_best_height_max_dates b
# MAGIC ON a.NHS_NUMBER_DEID = b.NHS_NUMBER_DEID AND a.HEIGHT_DATE = b.MAX_HEIGHT_DATE
# MAGIC GROUP BY a.NHS_NUMBER_DEID, a.HEIGHT_DATE

# COMMAND ----------

spark.sql("SELECT * FROM global_temp.ccu029_01_cohort_best_weight_and_height_table_name") \
  .unionByName(spark.sql("SELECT *, NULL AS WEIGHT_HEIGHT_DATE_DELTA, NULL AS BMI, NULL AS HEIGHT, NULL AS HEIGHT_DATE FROM global_temp.ccu029_01_best_weight")) \
  .unionByName(spark.sql("SELECT *, NULL AS WEIGHT_HEIGHT_DATE_DELTA, NULL AS BMI, NULL AS WEIGHT, NULL AS WEIGHT_DATE FROM global_temp.ccu029_01_best_height")) \
  .orderBy("NHS_NUMBER_DEID") \
  .createOrReplaceGlobalTempView("ccu029_01_best_weight_and_height_to_join")

if verbose:
  print("Joining the union of the weight, height and BMI info to the cohort...")

# COMMAND ----------

if test:
  display(spark.sql("SELECT COUNT(*), COUNT(DISTINCT NHS_NUMBER_DEID) FROM global_temp.ccu029_01_best_weight_and_height_to_join"))

# COMMAND ----------

if test:
  display(spark.sql("SELECT * FROM global_temp.ccu029_01_cohort_best_weight_and_height_table_name ORDER BY BMI DESC"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_w_bmi AS
# MAGIC SELECT
# MAGIC   a.*,
# MAGIC   WEIGHT_HEIGHT_DATE_DELTA,
# MAGIC   BMI,
# MAGIC   WEIGHT_DATE,
# MAGIC   HEIGHT_DATE,
# MAGIC   WEIGHT,
# MAGIC   HEIGHT,
# MAGIC   CASE WHEN BMI IS NOT NULL THEN 1 ELSE 0 END AS HAS_BMI,
# MAGIC   CASE WHEN HEIGHT IS NOT NULL THEN 1 ELSE 0 END AS HAS_HEIGHT,
# MAGIC   CASE WHEN WEIGHT IS NOT NULL THEN 1 ELSE 0 END AS HAS_WEIGHT
# MAGIC FROM global_temp.ccu029_01_cohort_for_bmi a
# MAGIC LEFT JOIN global_temp.ccu029_01_best_weight_and_height_to_join b
# MAGIC ON a.PERSON_ID_DEID = b.NHS_NUMBER_DEID

# COMMAND ----------

cohort_w_weights = spark.sql("SELECT * FROM global_temp.ccu029_01_w_bmi")

if verbose:
  print(f"Creating `{output_table_name}` with study start date == {study_start}")

cohort_w_weights.createOrReplaceGlobalTempView(output_table_name)
drop_table(output_table_name)
create_table(output_table_name)

table = spark.sql(f"SELECT * FROM dars_nic_391419_j3w9t_collab.{output_table_name}")
print(f"`{output_table_name}` has {table.count()} rows and {len(table.columns)} columns.")

# COMMAND ----------

if test:
  display(spark.sql("SELECT COVID_ADMISSION_IN_WINDOW, COUNT(*), SUM(CASE WHEN WEIGHT IS NOT NULL THEN 1 ELSE 0 END), SUM(CASE WHEN HEIGHT IS NOT NULL THEN 1 ELSE 0 END), SUM(CASE WHEN BMI IS NOT NULL THEN 1 ELSE 0 END), SUM(CASE WHEN BMI IS NOT NULL AND WEIGHT IS NOT NULL AND HEIGHT IS NOT NULL THEN 1 ELSE 0 END) FROM dars_nic_391419_j3w9t_collab.ccu029_01_wip_cohort_w_bmi GROUP BY COVID_ADMISSION_IN_WINDOW"))

# COMMAND ----------

if test:
  display(spark.sql("SELECT * FROM dars_nic_391419_j3w9t_collab.ccu029_01_wip_cohort_w_bmi"))

# COMMAND ----------

