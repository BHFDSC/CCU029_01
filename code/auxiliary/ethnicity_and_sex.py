# Databricks notebook source
# Convert from ethnicity codes to groups and descriptions, and convert numeric SEX to Male / Female

import io
import pandas as pd
from pyspark.sql.functions import col, create_map, lit
from itertools import chain

# From HES APC Data Dictionary (ONS 2011 census categories)
dict_ethnic = pd.read_csv("../../data/dict_ethnic.csv")

dict_ethnic = dict_ethnic.rename(columns={"Code" : "code",
                                          "Group" : "ethnic_group",
                                          "Description" : "ethnicity"})

# Convert to dictionary for mapping
mapping_ethnic_group = dict(zip(dict_ethnic['code'], dict_ethnic['ethnic_group']))
mapping_ethnicity = dict(zip(dict_ethnic['code'], dict_ethnic['ethnicity']))
mapping_sex = {1: 'Male', 2: 'Female'}

mapping_expr_ethnic_group = create_map([lit(x) for x in chain(*mapping_ethnic_group.items())])
mapping_expr_ethnicity = create_map([lit(x) for x in chain(*mapping_ethnicity.items())])
mapping_expr_sex = create_map([lit(x) for x in chain(*mapping_sex.items())])