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
  output_table_name = tests_output_table_name
except:
  raise ValueError("RUN TABLE_NAMES FIRST")

if verbose:
  print("Creating consistent and filtered version of the SGSS testing table (under 18s at study start, known sex)...")

# COMMAND ----------

# Again, standardise characteristics of records at a per-person level, then take the first positive test for each person in SGSS

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_skinny_dob_per_person AS
# MAGIC SELECT
# MAGIC   a.PERSON_ID_DEID,
# MAGIC   COALESCE(GREATEST(COALESCE(b.DATE_OF_BIRTH, c.DATE_OF_BIRTH), COALESCE(c.DATE_OF_BIRTH, b.DATE_OF_BIRTH)), a.DATE_OF_BIRTH) AS DATE_OF_BIRTH
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     PERSON_ID_DEID,
# MAGIC     MAX(DOB) AS DATE_OF_BIRTH
# MAGIC   FROM global_temp.ccu029_skinny
# MAGIC   GROUP BY PERSON_ID_DEID
# MAGIC ) a
# MAGIC LEFT JOIN (
# MAGIC   SELECT
# MAGIC     PERSON_ID_DEID,
# MAGIC     MAX(to_date(MYDOB, "MMyyyy")) AS DATE_OF_BIRTH
# MAGIC   FROM global_temp.ccu029_hes_apc
# MAGIC   GROUP BY PERSON_ID_DEID
# MAGIC ) b
# MAGIC ON a.PERSON_ID_DEID = b.PERSON_ID_DEID
# MAGIC LEFT JOIN (
# MAGIC   SELECT
# MAGIC     NHS_NUMBER_DEID,
# MAGIC     MAX(to_date(YEAR_MONTH_OF_BIRTH, "yyyy-MM")) AS DATE_OF_BIRTH
# MAGIC   FROM global_temp.ccu029_gdppr
# MAGIC   GROUP BY NHS_NUMBER_DEID
# MAGIC ) c
# MAGIC ON a.PERSON_ID_DEID = c.NHS_NUMBER_DEID;
# MAGIC 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_skinny_per_person AS
# MAGIC SELECT
# MAGIC   a.*,
# MAGIC   b.DATE_OF_BIRTH
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     PERSON_ID_DEID,
# MAGIC     FIRST(ETHNIC, TRUE) AS ETHNIC,
# MAGIC     FIRST(SEX, TRUE) AS SEX,
# MAGIC     FIRST(LSOA, TRUE) AS LSOA
# MAGIC   FROM (
# MAGIC     SELECT *
# MAGIC     FROM global_temp.ccu029_skinny
# MAGIC     WHERE SEX IN (1,2) AND LSOA LIKE "E%"
# MAGIC     DISTRIBUTE BY PERSON_ID_DEID
# MAGIC     SORT BY PERSON_ID_DEID, ETHNIC, SEX, LSOA
# MAGIC   )
# MAGIC   GROUP BY PERSON_ID_DEID
# MAGIC ) a
# MAGIC LEFT JOIN global_temp.ccu029_01_skinny_dob_per_person b
# MAGIC ON a.PERSON_ID_DEID = b.PERSON_ID_DEID;
# MAGIC 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_first_test_specimen_dates AS
# MAGIC SELECT
# MAGIC   PERSON_ID_DEID,
# MAGIC   MIN(Specimen_Date) AS POSITIVE_TEST_SPECIMEN_DATE
# MAGIC FROM global_temp.ccu029_sgss
# MAGIC WHERE Specimen_Date IS NOT NULL
# MAGIC GROUP BY PERSON_ID_DEID

# COMMAND ----------

full_cohort = spark.sql(f"""
SELECT
  a.*,
  POSITIVE_TEST_SPECIMEN_DATE,
  DATEDIFF(POSITIVE_TEST_SPECIMEN_DATE, DATE_OF_BIRTH) / 365.25 AS POSITIVE_TEST_SPECIMEN_AGE,
  CASE
    WHEN POSITIVE_TEST_SPECIMEN_DATE <= '2020-12-05' THEN "Original"
    WHEN POSITIVE_TEST_SPECIMEN_DATE <= '2021-01-02' THEN "Inter-Original-Alpha"
    WHEN POSITIVE_TEST_SPECIMEN_DATE <= '2021-05-01' THEN "Alpha"
    WHEN POSITIVE_TEST_SPECIMEN_DATE <= '2021-05-29' THEN "Inter-Alpha-Delta"
    WHEN POSITIVE_TEST_SPECIMEN_DATE <= '2021-12-11' THEN "Delta"
    WHEN POSITIVE_TEST_SPECIMEN_DATE <= '2021-12-25' THEN "Inter-Delta-Omicron"
    ELSE "Omicron"
  END AS POSITIVE_TEST_SPECIMEN_VARIANT_PERIOD,
  DECI_IMD
FROM global_temp.ccu029_01_skinny_per_person a
INNER JOIN global_temp.ccu029_01_first_test_specimen_dates b
ON a.PERSON_ID_DEID = b.PERSON_ID_DEID
LEFT JOIN (
  SELECT
    FIRST(DECI_IMD, TRUE) AS DECI_IMD,
    LSOA_CODE_2011
  FROM (
    SELECT *
    FROM dss_corporate.english_indices_of_dep_v02
    DISTRIBUTE BY LSOA_CODE_2011
    SORT BY LSOA_CODE_2011, IMD_YEAR DESC
  )
  GROUP BY LSOA_CODE_2011
) c
ON a.LSOA = c.LSOA_CODE_2011
WHERE POSITIVE_TEST_SPECIMEN_DATE <= '{study_end}'""")

# This is study end date such that tests can be linked after, but then the infection censoring at the next step ensures infections can only begin 42 days prior

if verbose:
  print(f"Creating `{output_table_name}` with study start date == {study_start}")

full_cohort.createOrReplaceGlobalTempView(output_table_name)
drop_table(output_table_name)
create_table(output_table_name)
optimise_table(output_table_name, 'PERSON_ID_DEID')

table = spark.sql(f"SELECT * FROM dars_nic_391419_j3w9t_collab.{output_table_name}")
print(f"`{output_table_name}` has {table.count()} rows and {len(table.columns)} columns.")

# COMMAND ----------

if test:
  display(spark.sql(f"SELECT COUNT(*), COUNT(DISTINCT PERSON_ID_DEID) FROM dars_nic_391419_j3w9t_collab.{output_table_name}"))

# COMMAND ----------

if test:
  display(spark.sql(f"SELECT * FROM dars_nic_391419_j3w9t_collab.{output_table_name}"))

# COMMAND ----------

