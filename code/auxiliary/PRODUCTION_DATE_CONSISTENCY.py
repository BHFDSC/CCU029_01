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

row = spark.sql("SELECT MAX(ProductionDate), MAX(archived_on) FROM dars_nic_391419_j3w9t_collab.hes_apc_all_years_archive").first()
extract_production_date = row[0]
extract_archived_on = row[1]

# COMMAND ----------

try:
  production_date
  if production_date == extract_production_date:
    print(f"Config specifies {production_date} as the production date, this is the latest (per the hes_apc_all_years_archive).")
  else:
    print(f"Config specifies {production_date} as the production date, NOTE the latest is: {extract_production_date}")
except:
  production_date = extract_production_date
  archived_on = extract_archived_on
  print(f"No production / archived-on date specified, using the latest which is: {extract_production_date}")

# COMMAND ----------

tables = {
  "gdppr_table" : "ccu029_gdppr",
  "sgss_table" : "ccu029_sgss",
  "deaths_table" : "ccu029_deaths",
  "hes_apc_table" : "ccu029_hes_apc",
  "hes_op_table" : "ccu029_hes_op",
  "hes_cc_table" : "ccu029_hes_cc",
  "hes_ae_table" : "ccu029_hes_ae"
}

archives = {
  "gdppr_table" : "gdppr_dars_nic_391419_j3w9t_archive",
  "sgss_table" : "sgss_dars_nic_391419_j3w9t_archive",
  "deaths_table" : "deaths_dars_nic_391419_j3w9t_archive",
  "hes_apc_table" : "hes_apc_all_years_archive",
  "hes_op_table" : "hes_op_all_years_archive",
  "hes_cc_table" : "hes_cc_all_years_archive",
  "hes_ae_table" : "hes_ae_all_years_archive"
}


def create_global_temp_tables(verb=False):
    spark.sql(f"""
    CREATE OR REPLACE GLOBAL TEMP VIEW {tables['gdppr_table']} AS
    SELECT * FROM dars_nic_391419_j3w9t_collab.gdppr_dars_nic_391419_j3w9t_archive
    WHERE archived_on = '{archived_on}'
    """)
    spark.sql(f"""
    CREATE OR REPLACE GLOBAL TEMP VIEW {tables['sgss_table']} AS
    SELECT * FROM dars_nic_391419_j3w9t_collab.sgss_dars_nic_391419_j3w9t_archive
    WHERE archived_on = '{archived_on}'
    """)
    spark.sql(f"""
    CREATE OR REPLACE GLOBAL TEMP VIEW {tables['deaths_table']} AS
    SELECT * FROM dars_nic_391419_j3w9t_collab.deaths_dars_nic_391419_j3w9t_archive
    WHERE archived_on = '{archived_on}'
    """)
    spark.sql(f"""
    CREATE OR REPLACE GLOBAL TEMP VIEW {tables['hes_apc_table']} AS
    SELECT * FROM dars_nic_391419_j3w9t_collab.hes_apc_all_years_archive
    WHERE archived_on = '{archived_on}'
    """)
    spark.sql(f"""
    CREATE OR REPLACE GLOBAL TEMP VIEW {tables['hes_op_table']} AS
    SELECT * FROM dars_nic_391419_j3w9t_collab.hes_op_all_years_archive
    WHERE archived_on = '{archived_on}'
    """)
    spark.sql(f"""
    CREATE OR REPLACE GLOBAL TEMP VIEW {tables['hes_cc_table']} AS
    SELECT * FROM dars_nic_391419_j3w9t_collab.hes_cc_all_years_archive
    WHERE archived_on = '{archived_on}'
    """)
    spark.sql(f"""
    CREATE OR REPLACE GLOBAL TEMP VIEW {tables['hes_ae_table']} AS
    SELECT * FROM dars_nic_391419_j3w9t_collab.hes_ae_all_years_archive
    WHERE archived_on = '{archived_on}'
    """)
    
    for table in tables.items():
      count = spark.sql(f"SELECT * FROM global_temp.{table[1]}").count()
      if not count:
        table_max_archived_on = spark.sql(f"SELECT MAX(archived_on) FROM dars_nic_391419_j3w9t_collab.{archives[table[0]]}").first()[0]
        print(f"There is no archive in `{archives[table[0]]}` for the selected date (`{archived_on}`), falling back to the archive from `{table_max_archived_on}`")
        spark.sql(f"""
        CREATE OR REPLACE GLOBAL TEMP VIEW {table[1]} AS
        SELECT * FROM dars_nic_391419_j3w9t_collab.{archives[table[0]]}
        WHERE archived_on = '{table_max_archived_on}'
        """)

create_global_temp_tables(verb=True)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_skinny AS
SELECT * FROM dars_nic_391419_j3w9t_collab.ccu029_manually_compiled_skinny
""")

if verbose:
  print(f"""
  TABLES GENERATED
  {', '.join(tables.values())}
  """)

# COMMAND ----------


