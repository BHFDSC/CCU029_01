# Databricks notebook source
# MAGIC %run ../config/quiet

# COMMAND ----------

# MAGIC %run ../auxiliary/helper_functions

# COMMAND ----------

# MAGIC %run ./TABLE_NAMES

# COMMAND ----------

# MAGIC %run ../auxiliary/UHC_functions

# COMMAND ----------

try:
  test
except:
  test = True
  
try:
  verbose
except:
  verbose = True

# COMMAND ----------

try:
  input_table_name = f"dars_nic_391419_j3w9t_collab.{cohort_w_zscores_output_table_name}"
  output_table_name = cohort_w_all_uhcs_output_table_name
except:
  raise ValueError("RUN TABLE_NAMES FIRST")

spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_cohort_for_md_uhcs AS
SELECT * FROM {input_table_name}
""")

if verbose:
  print("Reading in Kate's UHC definitions and generating query to identify them amongst the cohort...")

# COMMAND ----------

# Adding Underlying Health Conditions (UHCs) per Kate's spreadsheets.

# COMMAND ----------

cohort_with_md_uhcs = formulate_uhcs("MD", "global_temp.ccu029_01_cohort_for_md_uhcs")

# COMMAND ----------

if verbose:
  print(f"Creating `{cohort_w_md_uhcs_table_name}` with study start date == {study_start}")

cohort_with_md_uhcs.createOrReplaceGlobalTempView(cohort_w_md_uhcs_table_name)
drop_table(cohort_w_md_uhcs_table_name)
create_table(cohort_w_md_uhcs_table_name)

optimise_table(cohort_w_md_uhcs_table_name, "PERSON_ID_DEID")
create_temp_table(cohort_w_md_uhcs_table_name, "cohort_w_md_uhcs_table_name")

table = spark.sql(f"SELECT * FROM dars_nic_391419_j3w9t_collab.{cohort_w_md_uhcs_table_name}")
print(f"`{cohort_w_md_uhcs_table_name}` has {table.count()} rows and {len(table.columns)} columns.")

# COMMAND ----------

if verbose:
  print("Reading in the Green Book UHC definitions and generating query to identify them amongst the cohort...")

cohort_with_green_book_uhcs = formulate_uhcs("GREEN_BOOK", "global_temp.ccu029_01_cohort_w_md_uhcs_table_name")

# COMMAND ----------

if verbose:
  print(f"Creating `{output_table_name}` with study start date == {study_start}")

cohort_with_green_book_uhcs.createOrReplaceGlobalTempView(output_table_name)
drop_table(output_table_name)
create_table(output_table_name)

optimise_table(output_table_name, "PERSON_ID_DEID")

table = spark.sql(f"SELECT * FROM dars_nic_391419_j3w9t_collab.{output_table_name}")
print(f"`{output_table_name}` has {table.count()} rows and {len(table.columns)} columns.")

# COMMAND ----------

if test:
  display(spark.sql(f"SELECT COVID_ADMISSION_IN_WINDOW, COUNT(*), SUM(GREEN_BOOK_UHC), SUM(MD_UHC), SUM(GREATEST(MD_UHC, GREEN_BOOK_UHC)), SUM(GREEN_BOOK_RISK_FACTOR), SUM(MD_RISK_FACTOR), SUM(GREATEST(GREEN_BOOK_RISK_FACTOR, MD_RISK_FACTOR)) FROM dars_nic_391419_j3w9t_collab.{output_table_name} GROUP BY COVID_ADMISSION_IN_WINDOW"))

# COMMAND ----------

