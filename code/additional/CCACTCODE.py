# Databricks notebook source
# MAGIC %md
# MAGIC # Create `CCACTCODE` lookup
# MAGIC > A type of critical care activity provided to a patient during a critical care period. Up to 20 activity codes can be submitted on each daily neonatal/paediatric critical care record.  
# MAGIC 
# MAGIC Reference: [HES Technical Output Specification](https://digital.nhs.uk/data-and-information/data-tools-and-services/data-services/hospital-episode-statistics/hospital-episode-statistics-data-dictionary)  
# MAGIC <br>
# MAGIC * Pre-processing via python script (outside TRE) to parse `.csv` out of single cell (K10) in `HES+TOS+V1.5+-+published+M8.xlsx`

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU029/helper_functions

# COMMAND ----------

import io
import pandas as pd

df = pd.read_csv("../../data/CCACT_code_lookup.csv")
df = spark.createDataFrame(df)
df.createOrReplaceGlobalTempView("ccu029_lkp_ccactcode")
# drop_table("ccu029_lkp_ccactcode")
create_table("ccu029_lkp_ccactcode")
display(df)