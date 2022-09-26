# Databricks notebook source
# MAGIC %run ../config/loud

# COMMAND ----------

# MAGIC %run ../auxiliary/helper_functions

# COMMAND ----------

test = False
verbose = True

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ensure Dataset Production Date Consistency

# COMMAND ----------

# MAGIC %run ../auxiliary/PRODUCTION_DATE_CONSISTENCY $production_date=production_date

# COMMAND ----------

# MAGIC %md
# MAGIC # HES filtering and base cohort creation

# COMMAND ----------

# MAGIC %run ./1A_HES_Filtering

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding ICU information from HES CC

# COMMAND ----------

# MAGIC %run ./1B_HES_CC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Applying typing protocols to admissions

# COMMAND ----------

# MAGIC %run ./1C_Typing_Admissions

# COMMAND ----------

# MAGIC %md
# MAGIC # Filter Test Data from SGSS

# COMMAND ----------

# MAGIC %run ./2_SGSS_Filtering

# COMMAND ----------

# MAGIC %md
# MAGIC # Unify First Tests and All Admissions into First Infections with Admissions

# COMMAND ----------

# MAGIC %run ./3_Identify_All_Infections

# COMMAND ----------

# MAGIC %md
# MAGIC # Looking back through All Prior Admissions (APA)
# MAGIC 
# MAGIC As well as those in the periods nine months, two years and five years prior to admission for specific UHC definitions

# COMMAND ----------

# MAGIC %run ./4A_APA

# COMMAND ----------

# MAGIC %md
# MAGIC ### Determining BMI and Z-Scores

# COMMAND ----------

# MAGIC %run ./4B_BMI

# COMMAND ----------

# MAGIC %run ./4C_Z_Scores

# COMMAND ----------

# MAGIC %md
# MAGIC ### Identifying Kate-defined and Green Book UHCs

# COMMAND ----------

# MAGIC %run ./4D_UHCs

# COMMAND ----------

# MAGIC %md
# MAGIC # Add auxiliary data
# MAGIC 
# MAGIC ### Joining Civil Registration of Death (ONS)
# MAGIC 
# MAGIC Date of death and all causes

# COMMAND ----------

# MAGIC %run ./5A_Deaths

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vaccination Status (NHSD)

# COMMAND ----------

# MAGIC %run ./5B_Vaccinations

# COMMAND ----------

# MAGIC %md
# MAGIC # Finalising the cohort

# COMMAND ----------

# MAGIC %run ./6_Finalise_Cohort_and_Summarise

# COMMAND ----------

drop_all_tables(intermediary_tables)

# COMMAND ----------

