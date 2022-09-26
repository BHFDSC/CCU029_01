# Databricks notebook source
def get_col_names(table, without = []):
  return ", ".join([x for x in spark.sql(f"SELECT * FROM {table} LIMIT 1").schema.names if x not in without])

# COMMAND ----------

### 1. Sam Hollings
# Functions to create and drop (separate) a table from a pyspark data frame
# Builds delta tables to allow for optimisation
# Modifies owner to allow tables to be dropped
def create_table(table_name:str, database_name:str='dars_nic_391419_j3w9t_collab', select_sql_script:str=None, if_not_exists=True) -> None:
  """Will save to table from a global_temp view of the same name as the supplied table name (if no SQL script is supplied)
  Otherwise, can supply a SQL script and this will be used to make the table with the specificed name, in the specifcied database."""
  
  spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")
  
  if select_sql_script is None:
    select_sql_script = f"SELECT * FROM global_temp.{table_name}"
  
  if if_not_exists is True:
    if_not_exists_script=' IF NOT EXISTS'
  else:
    if_not_exists_script=''
  
  spark.sql(f"""CREATE TABLE {if_not_exists_script} {database_name}.{table_name} USING DELTA AS
                {select_sql_script}
             """)
  spark.sql(f"ALTER TABLE {database_name}.{table_name} OWNER TO {database_name}")
  
def drop_table(table_name:str, database_name:str='dars_nic_391419_j3w9t_collab', if_exists=True):
  if if_exists:
    IF_EXISTS = 'IF EXISTS'
  else: 
    IF_EXISTS = ''
  spark.sql(f"DROP TABLE {IF_EXISTS} {database_name}.{table_name}")

# COMMAND ----------

## 2. Chris Tomlinson
# Couple of useful functions for common tasks that I haven't found direct pyspark equivalents of
# Likely more efficient ways exist for those familiar with pyspark
# Code is likley to be heavily inspired by solutions seen on StackOverflow, unfortunately reference links not kept

from pyspark.sql.functions import lit, col, udf
from functools import reduce
from pyspark.sql import DataFrame
from datetime import datetime
from pyspark.sql.types import DateType

asDate = udf(lambda x: datetime.strptime(x, '%Y-%m-%d'), DateType())

# Pyspark has no .shape() function therefore define own
import pyspark

def spark_shape(self):
    return (self.count(), len(self.columns))
pyspark.sql.dataframe.DataFrame.shape = spark_shape

# Checking for duplicates
def checkOneRowPerPt(table, id):
  n = table.select(id).count()
  n_distinct = table.select(id).dropDuplicates([id]).count()
  print("N. of rows:", n)
  print("N. distinct ids:", n_distinct)
  print("Duplicates = ", n - n_distinct)
  if n > n_distinct:
    raise ValueError('Data has duplicates!')
    

# COMMAND ----------

def optimise_table(table_name:str, order_by:str):
  if order_by is None:
    spark.sql(f'OPTIMIZE dars_nic_391419_j3w9t_collab.{table_name}')
  else:
    spark.sql(f'OPTIMIZE dars_nic_391419_j3w9t_collab.{table_name} ZORDER BY {order_by}')

# COMMAND ----------

from pyspark.sql.functions import array, col, explode, lit, struct
from pyspark.sql import DataFrame
from typing import Iterable

def melt(df: DataFrame, 
        id_vars: Iterable[str], value_vars: Iterable[str], 
        var_name: str="variable", value_name: str="value") -> DataFrame:
    """Convert :class:`DataFrame` from wide to long format."""

    # Create array<struct<variable: str, value: ...>>
    _vars_and_vals = array(*(
        struct(lit(c).alias(var_name), col(c).alias(value_name)) 
        for c in value_vars))

    # Add to the DataFrame and explode
    _tmp = df.withColumn("_vars_and_vals", explode(_vars_and_vals))

    cols = id_vars + [
            col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]
    return _tmp.select(*cols)