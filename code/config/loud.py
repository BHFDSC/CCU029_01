# Databricks notebook source
# MAGIC %run /Workspaces//CCU029/01/config/quiet

# COMMAND ----------

print(f"""
Project ID (`project_id`): {project_id}
Question (`question_id`): {question_id}

Study Cohort Start Date (`study_start`): {study_start}
Study Cohort End Date (`study_end`): {study_end}

Production Date (`production_date`): {production_date}
""")