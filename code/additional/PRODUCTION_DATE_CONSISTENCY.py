# Databricks notebook source
# Specify the versions of tables to use, for purposes of reproducibility

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

sgss_production_date = spark.sql("SELECT MAX(ProductionDate) FROM dars_nic_391419_j3w9t_collab.sgss_dars_nic_391419_j3w9t_archive").first()[0]

# COMMAND ----------

try:
  production_date
  print(f"Config specifies {production_date} as the production date, the latest is: {sgss_production_date}")
except:
  production_date = sgss_production_date
  print(f"No production date specified, using the latest which is: {sgss_production_date}")

# COMMAND ----------

skinny_table = "ccu029_skinny_auto"
gdppr_table = "ccu029_gdppr"
sgss_table = "ccu029_sgss"
deaths_table = "ccu029_deaths"
hes_apc_table = "ccu029_hes_apc"
hes_op_table = "ccu029_hes_op"
hes_cc_table = "ccu029_hes_cc"
hes_ae_table = "ccu029_hes_ae"


def create_global_temp_tables(verb=False):
  if production_date == sgss_production_date:
    if verb:
      print("Using latest production date...")
    spark.sql(f"""
    CREATE OR REPLACE GLOBAL TEMP VIEW {skinny_table} AS
    SELECT * FROM dars_nic_391419_j3w9t_collab.patient_skinny_record
    """)
    spark.sql(f"""
    CREATE OR REPLACE GLOBAL TEMP VIEW {gdppr_table} AS
    SELECT * FROM dars_nic_391419_j3w9t.gdppr_dars_nic_391419_j3w9t
    """)
    spark.sql(f"""
    CREATE OR REPLACE GLOBAL TEMP VIEW {sgss_table} AS
    SELECT * FROM dars_nic_391419_j3w9t.sgss_dars_nic_391419_j3w9t
    """)
    spark.sql(f"""
    CREATE OR REPLACE GLOBAL TEMP VIEW {deaths_table} AS
    SELECT * FROM dars_nic_391419_j3w9t.deaths_dars_nic_391419_j3w9t
    """)
    spark.sql(f"""
    CREATE OR REPLACE GLOBAL TEMP VIEW {hes_apc_table} AS
    SELECT * FROM dars_nic_391419_j3w9t_collab.hes_apc_all_years
    """)
    spark.sql(f"""
    CREATE OR REPLACE GLOBAL TEMP VIEW {hes_op_table} AS
    SELECT * FROM dars_nic_391419_j3w9t_collab.hes_op_all_years
    """)
    spark.sql(f"""
    CREATE OR REPLACE GLOBAL TEMP VIEW {hes_cc_table} AS
    SELECT * FROM dars_nic_391419_j3w9t_collab.hes_cc_all_years
    """)
    spark.sql(f"""
    CREATE OR REPLACE GLOBAL TEMP VIEW {hes_ae_table} AS
    SELECT * FROM dars_nic_391419_j3w9t_collab.hes_ae_all_years
    """)
  else:
    if verb:
      print("Using old production date...")
    # Get historical table from the archive on that ProductionDate
    spark.sql(f"""
    CREATE OR REPLACE GLOBAL TEMP VIEW {skinny_table} AS
    SELECT * FROM dars_nic_391419_j3w9t_collab.curr302_patient_skinny_record_archive
    WHERE ProductionDate = '{production_date}'
    """)
    spark.sql(f"""
    CREATE OR REPLACE GLOBAL TEMP VIEW {gdppr_table} AS
    SELECT * FROM dars_nic_391419_j3w9t_collab.gdppr_dars_nic_391419_j3w9t_archive
    WHERE ProductionDate = '{production_date}'
    """)
    spark.sql(f"""
    CREATE OR REPLACE GLOBAL TEMP VIEW {sgss_table} AS
    SELECT * FROM dars_nic_391419_j3w9t_collab.sgss_dars_nic_391419_j3w9t_archive
    WHERE ProductionDate = '{production_date}'
    """)
    spark.sql(f"""
    CREATE OR REPLACE GLOBAL TEMP VIEW {deaths_table} AS
    SELECT * FROM dars_nic_391419_j3w9t_collab.deaths_dars_nic_391419_j3w9t_archive
    WHERE ProductionDate = '{production_date}'
    """)
    spark.sql(f"""
    CREATE OR REPLACE GLOBAL TEMP VIEW {hes_apc_table} AS
    SELECT * FROM dars_nic_391419_j3w9t_collab.hes_apc_all_years_archive
    WHERE ProductionDate = '{production_date}'
    """)
    spark.sql(f"""
    CREATE OR REPLACE GLOBAL TEMP VIEW {hes_op_table} AS
    SELECT * FROM dars_nic_391419_j3w9t_collab.hes_op_all_years_archive
    WHERE ProductionDate = '{production_date}'
    """)
    spark.sql(f"""
    CREATE OR REPLACE GLOBAL TEMP VIEW {hes_cc_table} AS
    SELECT * FROM dars_nic_391419_j3w9t_collab.hes_cc_all_years_archive
    WHERE ProductionDate = '{production_date}'
    """)
    spark.sql(f"""
    CREATE OR REPLACE GLOBAL TEMP VIEW {hes_ae_table} AS
    SELECT * FROM dars_nic_391419_j3w9t_collab.hes_ae_all_years_archive
    WHERE ProductionDate = '{production_date}'
    """)
    
create_global_temp_tables(verb=True)

if verbose:
  print(f"""
  TABLES GENERATED:

  {skinny_table}
  {gdppr_table}
  {sgss_table}
  {deaths_table}
  {hes_apc_table}
  {hes_op_table}
  {hes_cc_table}
  {hes_ae_table}

  """)


# COMMAND ----------

