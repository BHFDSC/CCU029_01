# Databricks notebook source
# MAGIC %run ../config/quiet

# COMMAND ----------

# MAGIC %run ../auxiliary/helper_functions

# COMMAND ----------

# MAGIC %run ./TABLE_NAMES

# COMMAND ----------

# MAGIC %run ../auxiliary/lms2z

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
  input_table_name = f"dars_nic_391419_j3w9t_collab.{cohort_w_bmi_output_table_name}"
  output_table_name = cohort_w_zscores_output_table_name
except:
  raise ValueError("RUN TABLE_NAMES FIRST")

spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_cohort_for_z_scores AS
SELECT * FROM {input_table_name}
""")

if verbose:
  print("Calculate Z-Scores relative to BMI- and weight-for-age...")

# COMMAND ----------

df = spark.sql("SELECT PERSON_ID_DEID, AGE, BMI, WEIGHT, SEX FROM global_temp.ccu029_01_cohort_for_z_scores ORDER BY PERSON_ID_DEID").toPandas()
ids = list(df["PERSON_ID_DEID"])
x = np.array(df["AGE"])
bmi = np.array(df["BMI"])
weight = np.array(df["WEIGHT"])
sex = np.array(df["SEX"]).astype(int)

# COMMAND ----------

bmi_z_scores = LMS2z(x, bmi, sex, "bmi", toz=True)
weight_z_scores = LMS2z(x, weight, sex, "wt", toz=True)

# COMMAND ----------

bmi_z_scores_df = spark.createDataFrame(pd.DataFrame({"PERSON_ID_DEID" : ids, "BMI_Z_SCORE" : bmi_z_scores})).replace(float('nan'), None)
weight_z_scores_df = spark.createDataFrame(pd.DataFrame({"PERSON_ID_DEID" : ids, "WEIGHT_Z_SCORE" : weight_z_scores})).replace(float('nan'), None)

# COMMAND ----------

bmi_z_scores_df.createOrReplaceGlobalTempView(cohort_bmi_zscores_table_name)
drop_table(cohort_bmi_zscores_table_name)
create_table(cohort_bmi_zscores_table_name)
create_temp_table(cohort_bmi_zscores_table_name, "cohort_bmi_zscores_table_name")

# COMMAND ----------

weight_z_scores_df.createOrReplaceGlobalTempView(cohort_weight_zscores_table_name)
drop_table(cohort_weight_zscores_table_name)
create_table(cohort_weight_zscores_table_name)
create_temp_table(cohort_weight_zscores_table_name, "cohort_weight_zscores_table_name")

# COMMAND ----------

cohort_w_z_scores = spark.sql("""
SELECT
  a.*,
  BMI_Z_SCORE,
  WEIGHT_Z_SCORE
FROM global_temp.ccu029_01_cohort_for_z_scores a
INNER JOIN global_temp.ccu029_01_cohort_bmi_zscores_table_name b
ON a.PERSON_ID_DEID = b.PERSON_ID_DEID
INNER JOIN global_temp.ccu029_01_cohort_weight_zscores_table_name c
ON a.PERSON_ID_DEID = c.PERSON_ID_DEID
""")

if verbose:
  print(f"Creating `{output_table_name}` with study start date == {study_start}")

cohort_w_z_scores.createOrReplaceGlobalTempView(output_table_name)
drop_table(output_table_name)
create_table(output_table_name)

table = spark.sql(f"SELECT * FROM dars_nic_391419_j3w9t_collab.{output_table_name}")
print(f"`{output_table_name}` has {table.count()} rows and {len(table.columns)} columns.")

# COMMAND ----------

