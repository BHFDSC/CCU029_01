# Databricks notebook source
# Define all of the table names used in the project, and choose which should be dropped after execution of the full pipeline

# COMMAND ----------

hes_apc_demographics_table_name = "ccu029_01_wip_hes_apc_demographics"
filtered_spells_table_name = "ccu029_01_wip_hes_apc_filtered_spells_agg"
admissions_output_table_name = "ccu029_01_wip_admissions"

admissions_w_icu_output_table_name = "ccu029_01_wip_admissions_w_icu"

admissions_typed_output_table_name = "ccu029_01_wip_admissions_typed"

tests_output_table_name = "ccu029_01_wip_first_positive_tests"

first_infections_output_table_name = "ccu029_01_wip_cohort"

cohort_hes_apc_lookback_table_name = "ccu029_01_wip_cohort_hes_apc_lookback"
cohort_hes_op_lookback_table_name = "ccu029_01_wip_cohort_hes_op_lookback"
cohort_gdppr_lookback_table_name = "ccu029_01_wip_cohort_gdppr_lookback"
cohort_w_lookback_output_table_name = "ccu029_01_wip_cohort_w_lookback"

cohort_codelist_weight_table_name = "ccu029_01_wip_cohort_codelist_weight_snomed"
cohort_codelist_height_table_name = "ccu029_01_wip_cohort_codelist_height_snomed"
cohort_all_weights_table_name = "ccu029_01_wip_cohort_all_weights"
cohort_all_heights_table_name = "ccu029_01_wip_cohort_all_heights"
cohort_best_weight_and_height_table_name = "ccu029_01_wip_cohort_best_weight_and_height"
cohort_w_bmi_output_table_name = "ccu029_01_wip_cohort_w_bmi"

cohort_bmi_zscores_table_name = "ccu029_01_wip_cohort_bmi_zscores"
cohort_weight_zscores_table_name = "ccu029_01_wip_cohort_weight_zscores"
cohort_w_zscores_output_table_name = "ccu029_01_wip_cohort_w_zscores"

cohort_w_md_uhcs_table_name = "ccu029_01_wip_cohort_w_md_uhcs"
cohort_w_all_uhcs_output_table_name = "ccu029_01_wip_cohort_w_all_uhcs"

cohort_w_deaths_output_table_name = "ccu029_01_wip_cohort_w_deaths"

cohort_w_vaccinations_output_table_name = "ccu029_01_wip_cohort_w_vaccinations"

final_cohort_output_table_name = "ccu029_01_complete_cohort"

intermediary_tables = [
  hes_apc_demographics_table_name,
  filtered_spells_table_name,
  cohort_hes_apc_lookback_table_name,
  cohort_hes_op_lookback_table_name,
  cohort_gdppr_lookback_table_name,
  cohort_codelist_weight_table_name,
  cohort_codelist_height_table_name,
  cohort_all_weights_table_name,
  cohort_all_heights_table_name,
  cohort_best_weight_and_height_table_name,
  cohort_bmi_zscores_table_name,
  cohort_weight_zscores_table_name,
  cohort_w_md_uhcs_table_name
]

# COMMAND ----------

def drop_all_tables(table_name_list):
  for table in table_name_list:
    drop_table(table)

# COMMAND ----------

# Creates a temporary table for consistent reference via global_temp throughout the code robust to small changes to table names set above. This is to allow for greater use of SQL code blocks rather than having to do everything with f strings
def create_temp_table(name, alias):
  spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_{alias} AS
SELECT * FROM dars_nic_391419_j3w9t_collab.{name}""")