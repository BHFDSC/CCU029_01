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
  input_table_name = f"dars_nic_391419_j3w9t_collab.{cohort_w_deaths_output_table_name}"
  output_table_name = cohort_w_vaccinations_output_table_name
except:
  raise ValueError("RUN TABLE_NAMES FIRST")

spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_cohort_for_vaccinations AS
SELECT * FROM {input_table_name}
""")

if verbose:
  print("Subsetting vaccination info to IDs from cohort and extracting dates...")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_filtered_vax AS
# MAGIC SELECT a.*
# MAGIC FROM dars_nic_391419_j3w9t.vaccine_status_dars_nic_391419_j3w9t a
# MAGIC INNER JOIN global_temp.ccu029_01_cohort_for_vaccinations b
# MAGIC ON a.PERSON_ID_DEID = b.PERSON_ID_DEID;
# MAGIC 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_filtered_vax_dates AS
# MAGIC SELECT
# MAGIC   vax1.PERSON_ID_DEID,
# MAGIC   vax1.date_vax AS FIRST_VAX_DATE,
# MAGIC   vax2.date_vax AS SECOND_VAX_DATE,
# MAGIC   booster.date_vax AS BOOSTER_VAX_DATE
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     PERSON_ID_DEID,
# MAGIC     MIN(to_date(substring(DATE_AND_TIME, 0,8), "yyyyMMdd")) as date_vax
# MAGIC   FROM global_temp.ccu029_01_filtered_vax
# MAGIC   WHERE VACCINATION_PROCEDURE_CODE = 1324681000000101
# MAGIC GROUP BY PERSON_ID_DEID
# MAGIC ) as vax1
# MAGIC LEFT JOIN (
# MAGIC   SELECT
# MAGIC     PERSON_ID_DEID,
# MAGIC     MIN(to_date(substring(DATE_AND_TIME, 0,8), "yyyyMMdd")) as date_vax
# MAGIC   FROM global_temp.ccu029_01_filtered_vax
# MAGIC   WHERE VACCINATION_PROCEDURE_CODE = 1324691000000104
# MAGIC   GROUP BY PERSON_ID_DEID
# MAGIC ) as vax2
# MAGIC ON vax1.PERSON_ID_DEID = vax2.PERSON_ID_DEID
# MAGIC LEFT JOIN (
# MAGIC   SELECT
# MAGIC     PERSON_ID_DEID,
# MAGIC     MIN(to_date(substring(DATE_AND_TIME, 0,8), "yyyyMMdd")) as date_vax
# MAGIC   FROM global_temp.ccu029_01_filtered_vax
# MAGIC   WHERE VACCINATION_PROCEDURE_CODE = 1362591000000103
# MAGIC   GROUP BY PERSON_ID_DEID
# MAGIC ) as booster
# MAGIC ON vax1.PERSON_ID_DEID = booster.PERSON_ID_DEID

# COMMAND ----------

if test:
  display(spark.sql("SELECT * FROM global_temp.ccu029_01_filtered_vax_dates"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_cohort_w_vaccinations AS
# MAGIC SELECT
# MAGIC   cohort.*,
# MAGIC   CASE
# MAGIC     WHEN DATE_ADD(BOOSTER_VAX_DATE, 14) <= INFECTION_DATE THEN "Booster Dose"
# MAGIC     WHEN DATE_ADD(SECOND_VAX_DATE, 14) <= INFECTION_DATE THEN "Second Dose"
# MAGIC     WHEN DATE_ADD(FIRST_VAX_DATE, 14) <= INFECTION_DATE THEN "First Dose"
# MAGIC     ELSE "Unvaccinated"
# MAGIC   END AS VACCINATION_STATUS_14_DAYS_PRIOR_TO_INFECTION,
# MAGIC   CASE WHEN DATE_ADD(FIRST_VAX_DATE, 14) <= INFECTION_DATE THEN 1 ELSE 0 END AS VACCINATED_14_DAYS_PRIOR_TO_INFECTION,
# MAGIC   FIRST_VAX_DATE,
# MAGIC   SECOND_VAX_DATE,
# MAGIC   BOOSTER_VAX_DATE
# MAGIC FROM global_temp.ccu029_01_cohort_for_vaccinations as cohort
# MAGIC LEFT JOIN global_temp.ccu029_01_filtered_vax_dates as vax
# MAGIC ON cohort.PERSON_ID_DEID = vax.PERSON_ID_DEID 

# COMMAND ----------

cohort_w_vaccinations = spark.sql("SELECT * FROM global_temp.ccu029_01_cohort_w_vaccinations")

if verbose:
  print(f"Creating `{output_table_name}` with study start date == {study_start}")

cohort_w_vaccinations.createOrReplaceGlobalTempView(output_table_name)
drop_table(output_table_name)
create_table(output_table_name)

optimise_table(output_table_name, "PERSON_ID_DEID")

table = spark.sql(f"SELECT * FROM dars_nic_391419_j3w9t_collab.{output_table_name}")
print(f"`{output_table_name}` has {table.count()} rows and {len(table.columns)} columns.")