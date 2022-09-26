# Databricks notebook source
# MAGIC %run ../project_config

# COMMAND ----------

question_id = '01'

production_date = '2022-06-29 00:00:00.000000'

study_start = '2020-07-01'
study_end = '2022-03-31'
uhc_date = study_end
case_hospitalisation_window = '42'
infection_censoring_date = datetime.strftime(datetime.strptime(study_end, "%Y-%m-%d") - timedelta(days=int(case_hospitalisation_window)), "%Y-%m-%d")