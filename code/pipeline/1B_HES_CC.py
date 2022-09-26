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
  input_table_name = f"dars_nic_391419_j3w9t_collab.{admissions_output_table_name}"
  output_table_name = admissions_w_icu_output_table_name
except:
  raise ValueError("RUN TABLE_NAMES FIRST")

spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_cohort_for_cc AS
SELECT * FROM {input_table_name}
""")

if verbose:
  print("Creating ICU Admission Flag...")

# COMMAND ----------

# # 1. Create ICU admission flag

# **HES CC**
# > Critical Care is a subset of APC data. It consists of Adult Critical Care from 2008-09 onwards (patients treated on adult critical care units), with Neonatal and Paediatric Critical Care included from 2017-18.   
# > The field Critical Care Period Type (CCPERTYPE) can be used to differentiate between the different types of critical care record.  
# > Each adult CC record represents a critical care period (from admission to a critical care location to discharge from that location).  
# > Each neonatal and paediatric CC record represents a calendar day (or part thereof) of neonatal or paediatric critical care.  


# **CCPERTYPE: Critical Care Period Type**  
# 01 = Adult (adult facilities, patients >= 19 years old on admission predominate)  
# 02 = Paediatric (children and young people facilities, patients ? 29 days to <19 years predominate)  
# 03 = Neonatal (neonatal facilities, patients <29 days on admission predominate)  
  
# **BESTMATCH**  
# > A flag stating whether the row represents the best match between the critical care and episode start and end dates for this critical care period. This flag is used to limit the data in instances where there is more than one row per critical care period. See Appendix C in the Critical Care 2008-09 publication for further details.  
# Value:  
# * Y or 1 = Row represents the best match between the critical care and episode dates
# * N or NULL = Row doesn?t represent the best match between the critical care and episode date

# Reference: [HES Technical Output Specification](https://digital.nhs.uk/data-and-information/data-tools-and-services/data-services/hospital-episode-statistics/hospital-episode-statistics-data-dictionary)

# COMMAND ----------

cohort_ICU = spark.sql(f"""
SELECT icu.ICU, cohort.*
FROM global_temp.ccu029_01_cohort_for_cc as cohort
LEFT JOIN (
  SELECT DISTINCT
    cohort_ids.PERSON_ID_DEID,
    cohort_ids.ADMIDATE,
    1 as ICU
  FROM (
    SELECT PERSON_ID_DEID, ADMIDATE, DISDATE
    FROM global_temp.ccu029_01_cohort_for_cc
  ) as cohort_ids
  INNER JOIN (
    SELECT PERSON_ID_DEID, to_date(CCSTARTDATE, 'yyyyMMdd') as CCSTARTDATE
    FROM global_temp.ccu029_hes_cc
    WHERE
      (CCPERTYPE = 2 OR CCPERTYPE = 3) -- PICU or NICU
      AND BESTMATCH = 1
  ) as cc
  ON 
    cohort_ids.PERSON_ID_DEID = cc.PERSON_ID_DEID
    AND cc.CCSTARTDATE >= cohort_ids.ADMIDATE
    AND (cc.CCSTARTDATE <= cohort_ids.DISDATE OR cohort_ids.DISDATE IS NULL)
) as icu
ON cohort.PERSON_ID_DEID = icu.PERSON_ID_DEID AND cohort.ADMIDATE = icu.ADMIDATE
""")

if test:
  display(cohort_ICU)

# COMMAND ----------

if verbose:
  print("Defining ICU Treatments...")

# COMMAND ----------

# # 2. ICU Treatments

# Uses Critical Care Activity Codes (`CCACTCODEn`)
# >A type of critical care activity provided to a patient during a critical care period. Up to 20 activity codes can be submitted on each daily neonatal/paediatric critical care record.  
  
# The dictionary to decode these is created in [`~/CCU029/00/CCACTODE`](https://db.core.data.digital.nhs.uk/#notebook/5445633/command/5445666) and stored in `dars_nic_391419_j3w9t_collab.ccu029_lkp_ccactcode`

# Plan:
# 1. Pivot all `CCACTCODEn` into `ID | CCACTIVDATE | CCACTCODE` + Join to a custom dictionary to decode CCACTCODE
# 2. Join to cohort with date conditions to ensure CCACTCODEs occurred within hospital admissions
# 3. Pivot to produce binary feature matrix
# 4. Join onto cohort to add covariates

# COMMAND ----------

# 2.1 Pivot all `CCATCODEn` to skinny record and join dictionary

# COMMAND ----------

import pyspark.sql.functions as f

# Load HES CC where admissions in PICU or NICU (CCPERTYPE = 2 | 3)
cc = spark.sql("""
SELECT *
FROM global_temp.ccu029_hes_cc
WHERE 
  (CCPERTYPE = 2 OR CCPERTYPE = 3)
  AND BESTMATCH = 1
""")

# Get CCACTCODEn cols (Thanks to Jake Kasan at NHS Digital for nice regex pivot script)
CCACTCODE_fields = [c for c in cc.columns if c.startswith("CCACTCODE")]
# Unlikley to use code position for this analysis but include nonetheless
CCACTCODE_and_n = [(c, int(c.lstrip("CCACTCODE"))) for c in CCACTCODE_fields]
CCACTCODE_columns = f.array(*(f.struct(f.col(c).alias("value"), f.lit(n).alias("n")) for c, n in CCACTCODE_and_n))

# Pivot
ccact_skinny = (
  cc
  .select("PERSON_ID_DEID",
          f.to_date(f.col("CCACTIVDATE"), "yyyyMMdd").alias("CCACTIVDATE"),
          f.explode(CCACTCODE_columns).alias("CCACTCODE"))
  .selectExpr("PERSON_ID_DEID", "CCACTIVDATE", "CCACTCODE.value as CCACTCODE", "CCACTCODE.n as CCACTCODEn")
  .filter("CCACTCODE IS NOT NULL")
  .withColumn('value', f.lit(1))
)

# Join onto custom dictionary
lkp_CCACTCODE = spark.sql("SELECT CCACTCODE, Short as CCACTCODE_desc FROM dars_nic_391419_j3w9t_collab.ccu029_lkp_ccactcode")
ccact_skinny = (ccact_skinny
            .join(lkp_CCACTCODE, "CCACTCODE", "left")
            # Use alias for ID to avoid duplication in join
            .selectExpr("PERSON_ID_DEID as ID_hosp_CC", "CCACTIVDATE", "CCACTCODE_desc as CCACTCODE", "value")
           )

if test:
  display(ccact_skinny)

# COMMAND ----------

# # 2.2 Join skinny to cohort with date conditions

# * Ensures that CCACTCODEs took place *within* hospital admission
# * WHERE CCACTIVDATE >= ADMIDATE AND CCACTIVDATE <= DISDATE

# COMMAND ----------

cohort_ids = spark.sql(f"""
SELECT
  PERSON_ID_DEID,
  ADMIDATE,
  DISDATE
FROM
  global_temp.ccu029_01_cohort_for_cc
""")

cc_cohort = (cohort_ids
             .join(ccact_skinny, 
                ((cohort_ids.PERSON_ID_DEID == ccact_skinny.ID_hosp_CC) & (ccact_skinny.CCACTIVDATE >= cohort_ids.ADMIDATE) & ((ccact_skinny.CCACTIVDATE <= cohort_ids.DISDATE) | cohort_ids.DISDATE.isNull())), 
                # Use inner join at this stage
                "inner")
             # Keep date cols for visual inspection
             .drop("ID_hosp_CC")
         )

# COMMAND ----------

# 2.3. Pivot into binary feature matrix

# COMMAND ----------

import databricks.koalas as ks

X = (cc_cohort
     .drop("DISDATE", "CCACTIVDATE")
     .to_koalas()
     .pivot_table(
       index=['PERSON_ID_DEID', 'ADMIDATE'], 
       columns='CCACTCODE', 
       values='value')
     .fillna(0)
     .reset_index()
     .to_spark()
     # Create an ICU definition from presence of CCACTCODEs
     .withColumn('ICU_CCACTIV', f.lit(1))
)

# COMMAND ----------

# # 2.4. Join onto main cohort table

# * `cohort_ICU` is the main cohort with binary ICU admission flag, as generated in Section 1

# COMMAND ----------

sub_cohort_w_icu = (
  cohort_ICU.select(["PERSON_ID_DEID", "ADMIDATE", "ICU"])
  .join(X, ["PERSON_ID_DEID", "ADMIDATE"], "left")
  .fillna(0, subset = X.schema.names + ["ICU"])
)
cohort_w_icu = spark.sql("SELECT * FROM global_temp.ccu029_01_cohort_for_cc").join(sub_cohort_w_icu, ["PERSON_ID_DEID", "ADMIDATE"], "left")

if verbose:
  print(f"Creating `{output_table_name}` with study start date == {study_start}")

cohort_w_icu.createOrReplaceGlobalTempView(output_table_name)
drop_table(output_table_name)
create_table(output_table_name)

optimise_table(output_table_name, "PERSON_ID_DEID")

table = spark.sql(f"SELECT * FROM dars_nic_391419_j3w9t_collab.{output_table_name}")
print(f"`{output_table_name}` has {table.count()} rows and {len(table.columns)} columns.")

# COMMAND ----------

if test:
  display(cohort_w_icu)

# COMMAND ----------

# Queries

# COMMAND ----------

if test:
  display(spark.sql(f"""
SELECT
  COUNT(*),
  COUNT(distinct PERSON_ID_DEID),
  SUM(ICU),
  SUM(ICU_CCACTIV),
  SUM(CC_O2),
  SUM(CC_nCPAP),
  SUM(CC_NIV),
  SUM(CC_IMV_ETT),
  SUM(CC_IMV_trache),
  SUM(CC_tracheal_tube),
  SUM(CC_jet_oscillator),
  SUM(CC_MCS)
FROM 
  dars_nic_391419_j3w9t_collab.{output_table_name}"""))

# COMMAND ----------

