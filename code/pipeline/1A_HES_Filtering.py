# Databricks notebook source
# MAGIC %run ../config/quiet

# COMMAND ----------

# MAGIC %run ../auxiliary/helper_functions

# COMMAND ----------

# MAGIC %run ../auxiliary/format_regex

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
  output_table_name = admissions_output_table_name
except:
  raise ValueError("RUN TABLE_NAMES FIRST")

if verbose:
  print("Create a consistent HES APC via the skinny table and looking at all records...")

# COMMAND ----------

if test:
  display(spark.sql(f"""
SELECT * FROM (
SELECT
  PERSON_ID_DEID,
  MAX(to_date(MYDOB, "MMyyyy")) AS DATE_OF_BIRTH
FROM global_temp.ccu029_hes_apc
GROUP BY PERSON_ID_DEID
)
WHERE DATE_OF_BIRTH IS NULL"""))

# COMMAND ----------

# The below is fairly self explanatory, we must assign consistent birth dates, ethnicity, LSOA and sex to each individual represented in the data, and a consistent discharge date for each person-admission pair, bias towards records that assign people known sex, in England, and are likely to have valid dates, as many records in HES have non-NULL dates that are far in the past as some NULL equivalent, but are clearly false e.g. 1800-01-01

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_hes_apc_date_of_birth_fixed AS
# MAGIC SELECT
# MAGIC   a.PERSON_ID_DEID,
# MAGIC   COALESCE(a.DATE_OF_BIRTH, b.DATE_OF_BIRTH, c.DATE_OF_BIRTH) AS DATE_OF_BIRTH
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     PERSON_ID_DEID,
# MAGIC     MAX(to_date(MYDOB, "MMyyyy")) AS DATE_OF_BIRTH
# MAGIC   FROM global_temp.ccu029_hes_apc
# MAGIC   GROUP BY PERSON_ID_DEID
# MAGIC ) a
# MAGIC LEFT JOIN (
# MAGIC   SELECT
# MAGIC     NHS_NUMBER_DEID,
# MAGIC     MAX(to_date(YEAR_MONTH_OF_BIRTH, "yyyy-MM")) AS DATE_OF_BIRTH
# MAGIC   FROM global_temp.ccu029_gdppr
# MAGIC   GROUP BY NHS_NUMBER_DEID
# MAGIC ) b
# MAGIC ON a.PERSON_ID_DEID = b.NHS_NUMBER_DEID
# MAGIC LEFT JOIN (
# MAGIC   SELECT
# MAGIC     PERSON_ID_DEID,
# MAGIC     MAX(DOB) AS DATE_OF_BIRTH
# MAGIC   FROM global_temp.ccu029_skinny
# MAGIC   GROUP BY PERSON_ID_DEID
# MAGIC ) c
# MAGIC ON a.PERSON_ID_DEID = c.PERSON_ID_DEID;
# MAGIC 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_hes_apc_disdate_fixed AS
# MAGIC SELECT
# MAGIC   PERSON_ID_DEID,
# MAGIC   ADMIDATE,
# MAGIC   MAX(DISDATE) AS DISDATE
# MAGIC FROM global_temp.ccu029_hes_apc
# MAGIC GROUP BY PERSON_ID_DEID, ADMIDATE;
# MAGIC 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_hes_apc_fixed_per_person_demographics AS
# MAGIC SELECT
# MAGIC   PERSON_ID_DEID,
# MAGIC   FIRST(LSOA, TRUE) AS LSOA,
# MAGIC   FIRST(ETHNIC, TRUE) AS ETHNIC,
# MAGIC   FIRST(SEX, TRUE) AS SEX,
# MAGIC   FIRST(TRUST_CODE, TRUE) AS TRUST_CODE
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     a.PERSON_ID_DEID,
# MAGIC     a.ADMIDATE,
# MAGIC     COALESCE(b.LSOA, a.LSOA11) AS LSOA,
# MAGIC     COALESCE(b.ETHNIC, a.ETHNOS) AS ETHNIC,
# MAGIC     COALESCE(b.SEX, a.SEX) AS SEX,
# MAGIC     a.PROCODE3 AS TRUST_CODE
# MAGIC   FROM (SELECT DISTINCT PERSON_ID_DEID, ADMIDATE, LSOA11, ETHNOS, SEX, PROCODE3 FROM global_temp.ccu029_hes_apc) a
# MAGIC   LEFT JOIN global_temp.ccu029_skinny b
# MAGIC   ON a.PERSON_ID_DEID = b.PERSON_ID_DEID
# MAGIC   WHERE COALESCE(b.LSOA, a.LSOA11) LIKE "E%" AND COALESCE(b.SEX, a.SEX) IN (1,2)
# MAGIC   DISTRIBUTE BY a.PERSON_ID_DEID
# MAGIC   SORT BY a.PERSON_ID_DEID, a.ADMIDATE DESC, COALESCE(b.LSOA, a.LSOA11), COALESCE(b.ETHNIC, a.ETHNOS), COALESCE(b.SEX, a.SEX), TRUST_CODE
# MAGIC )
# MAGIC GROUP BY PERSON_ID_DEID;
# MAGIC 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_hes_apc_fixed_demographics AS
# MAGIC SELECT
# MAGIC   a.PERSON_ID_DEID,
# MAGIC   a.ADMIDATE,
# MAGIC   a.DISDATE,
# MAGIC   b.LSOA,
# MAGIC   b.ETHNIC,
# MAGIC   b.SEX,
# MAGIC   c.DATE_OF_BIRTH,
# MAGIC   DATEDIFF(a.ADMIDATE, c.DATE_OF_BIRTH) / 365.25 AS ADMISSION_AGE,
# MAGIC   b.TRUST_CODE
# MAGIC FROM global_temp.ccu029_01_hes_apc_disdate_fixed a
# MAGIC INNER JOIN global_temp.ccu029_01_hes_apc_fixed_per_person_demographics b
# MAGIC ON a.PERSON_ID_DEID = b.PERSON_ID_DEID
# MAGIC INNER JOIN global_temp.ccu029_01_hes_apc_date_of_birth_fixed c
# MAGIC ON a.PERSON_ID_DEID = c.PERSON_ID_DEID

# COMMAND ----------

if verbose:
  print(f"Filtering HES APC to valid records occurring before the end of the study period ({study_end}) and after a reasonable start date ({hospitalisation_start_date}) for the pandemic...")

# COMMAND ----------

hes_apc_fixed_censored = spark.sql(f"""
SELECT
  PERSON_ID_DEID,
  ADMIDATE,
  CASE WHEN DISDATE > '{study_end}' THEN NULL ELSE DISDATE END AS DISDATE,
  LSOA,
  ETHNIC,
  SEX,
  DATE_OF_BIRTH,
  ADMISSION_AGE,
  TRUST_CODE
FROM global_temp.ccu029_01_hes_apc_fixed_demographics
WHERE
  ADMIDATE BETWEEN '{hospitalisation_start_date}' AND '{study_end}'
  AND (ADMIDATE <= DISDATE OR DISDATE IS NULL)
""")

if verbose:
  print(f"Creating `{hes_apc_demographics_table_name}` with study start date == {study_start}...")

hes_apc_fixed_censored.createOrReplaceGlobalTempView(hes_apc_demographics_table_name)
drop_table(hes_apc_demographics_table_name)
create_table(hes_apc_demographics_table_name)
create_temp_table(hes_apc_demographics_table_name, "hes_apc_demographics_table_name")

table = spark.sql(f"SELECT * FROM dars_nic_391419_j3w9t_collab.{hes_apc_demographics_table_name}")
print(f"`{hes_apc_demographics_table_name}` has {table.count()} rows and {len(table.columns)} columns.")

# COMMAND ----------

if test:
  display(spark.sql("SELECT COUNT(*), COUNT(DISTINCT PERSON_ID_DEID, ADMIDATE) FROM global_temp.ccu029_01_hes_apc_demographics_table_name"))

# COMMAND ----------

if verbose:
  print("Collapsing records into individual spells...")

# COMMAND ----------

# We group by all of the characteristics standardised above to convert HES APC which is an episode-level dataset, into a spell-level one, and concatenate and collect all of the codes for each episode in a spell into a single record

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_hes_apc_filtered_spells_raw AS
# MAGIC SELECT
# MAGIC   a.PERSON_ID_DEID,
# MAGIC   a.ADMIDATE,
# MAGIC   a.DISDATE,
# MAGIC   a.DATE_OF_BIRTH,
# MAGIC   a.ADMISSION_AGE,
# MAGIC   a.SEX,
# MAGIC   a.ETHNIC,
# MAGIC   a.LSOA,
# MAGIC   a.TRUST_CODE,
# MAGIC   COUNT(*) AS EPICOUNT,
# MAGIC   CASE WHEN SUM(CASE WHEN CLASSPAT = 2 THEN 1 ELSE 0 END) >= 1 AND DATEDIFF(a.DISDATE, a.ADMIDATE) == 0 THEN 1 ELSE 0 END AS DAY_CASE,
# MAGIC   CONCAT_WS(',', COLLECT_LIST(COALESCE(EPIORDER, '?'))) AS ALL_EPIORDERS,
# MAGIC   CONCAT_WS(',', COLLECT_LIST(COALESCE(EPISTART, '?'))) AS ALL_EPISTARTS,
# MAGIC   CONCAT_WS(',', FLATTEN(COLLECT_LIST(SPLIT(DIAG_4_CONCAT, ',')))) AS DIAG_4_CONCAT,
# MAGIC   CONCAT_WS(',', FLATTEN(COLLECT_LIST(SPLIT(NULLIF(CONCAT_WS(',', DIAG_4_02, DIAG_4_03, DIAG_4_04, DIAG_4_05, DIAG_4_06, DIAG_4_07, DIAG_4_08, DIAG_4_09, DIAG_4_10, DIAG_4_11, DIAG_4_12, DIAG_4_13, DIAG_4_14, DIAG_4_15, DIAG_4_16, DIAG_4_17, DIAG_4_18, DIAG_4_19, DIAG_4_20), ''), ',')))) AS DIAG_4_CONCAT_SECONDARY,
# MAGIC   CONCAT_WS(',', COLLECT_LIST(DIAG_4_01)) AS DIAG_4_CONCAT_PRIMARY,
# MAGIC   CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(DIAG_4_CONCAT, ','))))) AS UNIQUE_DIAG_4_CONCAT,
# MAGIC   CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(NULLIF(CONCAT_WS(',', DIAG_4_02, DIAG_4_03, DIAG_4_04, DIAG_4_05, DIAG_4_06, DIAG_4_07, DIAG_4_08, DIAG_4_09, DIAG_4_10, DIAG_4_11, DIAG_4_12, DIAG_4_13, DIAG_4_14, DIAG_4_15, DIAG_4_16, DIAG_4_17, DIAG_4_18, DIAG_4_19, DIAG_4_20), ''), ','))))) AS UNIQUE_DIAG_4_CONCAT_SECONDARY,
# MAGIC   CONCAT_WS(',', ARRAY_DISTINCT(COLLECT_LIST(DIAG_4_01))) AS UNIQUE_DIAG_4_CONCAT_PRIMARY,
# MAGIC   CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(NULLIF(OPERTN_3_CONCAT, '-'), ','))))) AS OPERTN_3_CONCAT,
# MAGIC   CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(NULLIF(OPERTN_4_CONCAT, '-'), ','))))) AS OPERTN_4_CONCAT,
# MAGIC   CASE WHEN SUM(CASE WHEN (EPISTART <= DATE_ADD(a.ADMIDATE, 6) AND (DIAG_4_01 RLIKE "U071|U072|U073|U074" OR DIAG_4_CONCAT RLIKE "U071|U072")) THEN 1 ELSE 0 END) >= 1 THEN 1 ELSE 0 END AS DEFINITE_COVID_CODES_PRE_DAY_8,
# MAGIC   CASE WHEN SUM(CASE WHEN (EPISTART <= DATE_ADD(a.ADMIDATE, 6) AND (DIAG_4_CONCAT RLIKE "U071|U072|U073|U074")) THEN 1 ELSE 0 END) >= 1 THEN 1 ELSE 0 END AS ALL_COVID_CODES_PRE_DAY_8
# MAGIC FROM global_temp.ccu029_01_hes_apc_demographics_table_name a
# MAGIC INNER JOIN (SELECT * FROM global_temp.ccu029_hes_apc DISTRIBUTE BY PERSON_ID_DEID SORT BY PERSON_ID_DEID, EPIORDER ASC) b
# MAGIC ON a.PERSON_ID_DEID = b.PERSON_ID_DEID AND a.ADMIDATE = b.ADMIDATE
# MAGIC GROUP BY a.PERSON_ID_DEID, a.ADMIDATE, a.DISDATE, a.DATE_OF_BIRTH, a.ADMISSION_AGE, a.SEX, a.ETHNIC, a.LSOA, a.TRUST_CODE;

# COMMAND ----------

# After converting to a spell-level dataset, calculate and derive a number of other variables using the newly collected spell-level information

filtered_spells = spark.sql(f"""
SELECT
  a.*,
  CASE WHEN DISDATE IS NULL THEN 1 ELSE 0 END AS STILL_IN_HOSPITAL,
  DATEDIFF(DISDATE, ADMIDATE) + 1 as LENGTH_OF_STAY,
  CASE
    WHEN ADMIDATE <= '2020-12-05' THEN "Original"
    WHEN ADMIDATE <= '2021-01-02' THEN "Inter-Original-Alpha"
    WHEN ADMIDATE <= '2021-05-01' THEN "Alpha"
    WHEN ADMIDATE <= '2021-05-29' THEN "Inter-Alpha-Delta"
    WHEN ADMIDATE <= '2021-12-11' THEN "Delta"
    WHEN ADMIDATE <= '2021-12-25' THEN "Inter-Delta-Omicron"
    ELSE "Omicron"
  END AS ADMISSION_VARIANT_PERIOD,
  -- OPERATIONAL CODES
  -- Presence of CPAP/NIV/IMV implies Oxygen:
  CASE WHEN OPERTN_3_CONCAT RLIKE "X52|E856|E852|E851" THEN 1 ELSE 0 END AS PRESENCE_OXYGEN,
  CASE WHEN OPERTN_4_CONCAT LIKE "%E856%" THEN 1 ELSE 0 END AS PRESENCE_CPAP,
  CASE WHEN OPERTN_4_CONCAT LIKE "%E852%" THEN 1 ELSE 0 END AS PRESENCE_NIV,
  CASE WHEN OPERTN_4_CONCAT LIKE "%E851%" THEN 1 ELSE 0 END AS PRESENCE_IMV,
  CASE WHEN OPERTN_4_CONCAT LIKE "%X581%" THEN 1 ELSE 0 END AS PRESENCE_ECMO,
  CASE WHEN DIAG_4_CONCAT_PRIMARY RLIKE "U071|U072|U073|U074" THEN 1 ELSE 0 END AS COVID_PRIMARY_HOSPITALISATION, 
  CASE WHEN DIAG_4_CONCAT_SECONDARY RLIKE "U071|U072" THEN 1 ELSE 0 END AS COVID_DEFINITE_SECONDARY_HOSPITALISATION,
  CASE WHEN DIAG_4_CONCAT_SECONDARY RLIKE "U073|U074" THEN 1 ELSE 0 END AS COVID_OTHER_SECONDARY_HOSPITALISATION,
  CASE WHEN DIAG_4_CONCAT_SECONDARY RLIKE "U071|U072|U073|U074" THEN 1 ELSE 0 END AS COVID_SECONDARY_HOSPITALISATION,
  CASE WHEN DIAG_4_CONCAT_PRIMARY RLIKE "U075|M303|R65" THEN 1 ELSE 0 END AS PIMS_PRIMARY_HOSPITALISATION,
  CASE WHEN DIAG_4_CONCAT_SECONDARY RLIKE "U075|M303|R65" THEN 1 ELSE 0 END AS PIMS_SECONDARY_HOSPITALISATION,
  CASE WHEN (
    DIAG_4_CONCAT RLIKE 'U075'
    AND ADMIDATE >= '{pims_defn_date}'
  ) OR (
    DIAG_4_CONCAT RLIKE 'M303|R65'
    AND DIAG_4_CONCAT NOT RLIKE {convert_list_to_regex_string('A00,A01,A02,A03,A04,A05,A06,A07,A080,A081,A082,A17,A18,A19,A2,A3,A4,A5,A6,A7,A80,A810,A811,A812,A82,A83,A84,A85,A870,A871,A880,A881,A9,B01,B02,B03,B04,B05,B06,B07,B08,B15,B16,B17,B18,B2,B300,B301,B303,B330,B331,B333,B334,B340,B341,B343,B344,B4,B5,B6,B7,B8,B90,B91,B92,B94,B95,B96,B970,B971,B973,B974,B975,B976,B977,B98,C,D37,D38,D4,D5,D60,D61,D62,D63,D65,D66,D67,D8,E883,G00,G01,G02,G030,G031,G032,G041,G042,G05,G06,G07,G08,I0,I31,132,I35,I36,I37,I38,I39,Q2,J020,J030,J09,J10,J11,J120,J121,J122,J123,J13,J14,J15,J160,J17,J200,J201,J202,J203,J204,J205,J206,J207,J210,J211,J218,J36,J390,J391,J69,J85,J86,J94,J95,L0,M0,M01,M03,O,P0,P1,P21,P23,P24,P26,P350,P351,P352,P354,P354,P36,P37,P50,P51,P54,P75,P76,P77,P780,P781,P782,P783,P960,R02,R572,S,T,V,Z958,Z982,Z948')}
    AND ADMIDATE >= '{pims_defn_date}'
  ) THEN 1 ELSE 0 END AS PIMS_REQUIREMENT,
  b.DECI_IMD AS DECI_IMD
FROM global_temp.ccu029_01_hes_apc_filtered_spells_raw a
-- The join to IMD data is done like this because there are multiple rows per LSOA_CODE_2011, we order by IMD_YEAR DESC and take the first DECI_IMD value to hopefully get the most recent
LEFT JOIN (
  SELECT
    FIRST(DECI_IMD, TRUE) AS DECI_IMD,
    LSOA_CODE_2011
  FROM (
    SELECT *
    FROM dss_corporate.english_indices_of_dep_v02
    DISTRIBUTE BY LSOA_CODE_2011
    SORT BY LSOA_CODE_2011, IMD_YEAR DESC
  )
  GROUP BY LSOA_CODE_2011
) b
ON a.LSOA = b.LSOA_CODE_2011""")

if verbose:
  print(f"Creating `{filtered_spells_table_name}` with study start date == {study_start}")

filtered_spells.createOrReplaceGlobalTempView(filtered_spells_table_name)
drop_table(filtered_spells_table_name)
create_table(filtered_spells_table_name)

optimise_table(filtered_spells_table_name, 'PERSON_ID_DEID')
create_temp_table(filtered_spells_table_name, "filtered_spells_table_name")

table = spark.sql(f"SELECT * FROM dars_nic_391419_j3w9t_collab.{filtered_spells_table_name}")
print(f"`{filtered_spells_table_name}` has {table.count()} rows and {len(table.columns)} columns.")

# COMMAND ----------

if test:
  display(spark.sql(f"SELECT LENGTH_OF_STAY, * FROM dars_nic_391419_j3w9t_collab.{filtered_spells_table_name} ORDER BY LENGTH_OF_STAY DESC"))

# COMMAND ----------

# Positive tests

# We join the testing data by taking the minimum specimen date that is <14 days before admission for each patient-admission pair. We can then determine nosocomial cases and filter by how long *after* admission this test date is to see whether we want to class this as a "COVID-positive admission". The positive test should occur WITHIN the admission, i.e. before discharge.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_sgss_censored AS
SELECT * FROM global_temp.ccu029_sgss WHERE Specimen_Date <= '{study_end}'""")

if test:
  display(spark.sql("""
  SELECT
    a.PERSON_ID_DEID,
    ADMIDATE,
    MIN(Specimen_Date) AS EARLIEST_COMPLIANT_SPECIMEN_DATE,
    DATEDIFF(MIN(Specimen_Date), ADMIDATE) AS DIFF
  FROM global_temp.ccu029_01_filtered_spells_table_name a
  LEFT JOIN global_temp.ccu029_01_sgss_censored b
  ON a.PERSON_ID_DEID = b.PERSON_ID_DEID
  WHERE Specimen_Date >= DATE_ADD(ADMIDATE, -13)
  GROUP BY a.PERSON_ID_DEID, ADMIDATE
  ORDER BY DIFF ASC
  -- ORDER BY DIFF DESC"""))

# COMMAND ----------

if verbose:
  print("Joining testing data to the filtered HES APC...")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_hes_apc_filtered_tests AS
# MAGIC SELECT
# MAGIC   x.*,
# MAGIC   EARLIEST_COMPLIANT_SPECIMEN_DATE,
# MAGIC   DATEDIFF(EARLIEST_COMPLIANT_SPECIMEN_DATE, x.ADMIDATE) AS COMPLIANT_COVID_TEST_ADMISSION_DELTA,
# MAGIC   CASE WHEN EARLIEST_COMPLIANT_SPECIMEN_DATE IS NOT NULL AND (EARLIEST_COMPLIANT_SPECIMEN_DATE <= DATE_ADD(x.ADMIDATE, 6) AND (EARLIEST_COMPLIANT_SPECIMEN_DATE <= DISDATE OR DISDATE IS NULL)) THEN 1 ELSE 0 END AS POSITIVE_COVID_TEST_IN_WINDOW,
# MAGIC   CASE WHEN EARLIEST_COMPLIANT_SPECIMEN_DATE IS NOT NULL AND (EARLIEST_COMPLIANT_SPECIMEN_DATE >= DATE_ADD(x.ADMIDATE, -5) AND (EARLIEST_COMPLIANT_SPECIMEN_DATE <= DISDATE OR DISDATE IS NULL)) THEN 1 ELSE 0 END AS POSITIVE_COVID_TEST_IN_UKHSA_WINDOW,
# MAGIC   CASE WHEN EARLIEST_COMPLIANT_SPECIMEN_DATE IS NOT NULL AND (EARLIEST_COMPLIANT_SPECIMEN_DATE <= DISDATE OR DISDATE IS NULL) THEN 1 ELSE 0 END AS POSITIVE_COVID_TEST_INCLUSION,
# MAGIC   -- Nosocomial case definitions
# MAGIC   CASE
# MAGIC     WHEN EARLIEST_COMPLIANT_SPECIMEN_DATE IS NOT NULL AND (EARLIEST_COMPLIANT_SPECIMEN_DATE <= DATE_ADD(x.ADMIDATE, 1) AND (EARLIEST_COMPLIANT_SPECIMEN_DATE <= DISDATE OR DISDATE IS NULL)) THEN "Community Onset Community Acquired"
# MAGIC     WHEN EARLIEST_COMPLIANT_SPECIMEN_DATE IS NOT NULL AND (EARLIEST_COMPLIANT_SPECIMEN_DATE <= DATE_ADD(x.ADMIDATE, 6) AND (EARLIEST_COMPLIANT_SPECIMEN_DATE <= DISDATE OR DISDATE IS NULL)) THEN "Hospital Onset (Indeterminable) Hospital Associated"
# MAGIC     WHEN EARLIEST_COMPLIANT_SPECIMEN_DATE IS NOT NULL AND (EARLIEST_COMPLIANT_SPECIMEN_DATE <= DATE_ADD(x.ADMIDATE, 13) AND(EARLIEST_COMPLIANT_SPECIMEN_DATE <= DISDATE OR DISDATE IS NULL)) THEN "Hospital Onset (Probable) Hospital Associated"
# MAGIC     WHEN EARLIEST_COMPLIANT_SPECIMEN_DATE IS NOT NULL AND (EARLIEST_COMPLIANT_SPECIMEN_DATE <= DISDATE OR DISDATE IS NULL) THEN "Hospital Onset (Definite) Hospital Associated"
# MAGIC     WHEN EARLIEST_COMPLIANT_SPECIMEN_DATE IS NOT NULL AND (EARLIEST_COMPLIANT_SPECIMEN_DATE <= DATE_ADD(DISDATE, 14)) THEN "Community Onset (Possible) Hospital Associated"
# MAGIC     ELSE "N/A"
# MAGIC   END AS NOSOCOMIAL_CASE_DEFINITION,
# MAGIC   CASE WHEN EARLIEST_COMPLIANT_SPECIMEN_DATE IS NOT NULL AND (EARLIEST_COMPLIANT_SPECIMEN_DATE >= DATE_ADD(x.ADMIDATE, 7) AND DEFINITE_COVID_CODES_PRE_DAY_8 == 0 AND (EARLIEST_COMPLIANT_SPECIMEN_DATE <= DISDATE OR DISDATE IS NULL)) THEN 1 ELSE 0 END AS NOSOCOMIAL_FLAG,
# MAGIC   CASE WHEN EARLIEST_COMPLIANT_SPECIMEN_DATE IS NOT NULL AND (EARLIEST_COMPLIANT_SPECIMEN_DATE >= DATE_ADD(x.ADMIDATE, 7) AND ALL_COVID_CODES_PRE_DAY_8 == 0 AND (EARLIEST_COMPLIANT_SPECIMEN_DATE <= DISDATE OR DISDATE IS NULL)) THEN 1 ELSE 0 END AS NOSOCOMIAL_FLAG_ALT,
# MAGIC   CASE WHEN EARLIEST_COMPLIANT_SPECIMEN_DATE IS NOT NULL AND (EARLIEST_COMPLIANT_SPECIMEN_DATE <= DATE_ADD(x.ADMIDATE, 1) AND (EARLIEST_COMPLIANT_SPECIMEN_DATE <= DISDATE OR DISDATE IS NULL)) THEN 1 ELSE 0 END AS CO_CA,
# MAGIC   CASE WHEN EARLIEST_COMPLIANT_SPECIMEN_DATE IS NOT NULL AND (EARLIEST_COMPLIANT_SPECIMEN_DATE >= DATE_ADD(x.ADMIDATE, 2) AND EARLIEST_COMPLIANT_SPECIMEN_DATE <= DATE_ADD(x.ADMIDATE, 6) AND (EARLIEST_COMPLIANT_SPECIMEN_DATE <= DISDATE OR DISDATE IS NULL)) THEN 1 ELSE 0 END AS HO_INDETERMINABLE_HA,
# MAGIC   CASE WHEN EARLIEST_COMPLIANT_SPECIMEN_DATE IS NOT NULL AND (EARLIEST_COMPLIANT_SPECIMEN_DATE >= DATE_ADD(x.ADMIDATE, 7) AND EARLIEST_COMPLIANT_SPECIMEN_DATE <= DATE_ADD(x.ADMIDATE, 13) AND (EARLIEST_COMPLIANT_SPECIMEN_DATE <= DISDATE OR DISDATE IS NULL)) THEN 1 ELSE 0 END AS HO_PROBABLE_HA,
# MAGIC   CASE WHEN EARLIEST_COMPLIANT_SPECIMEN_DATE IS NOT NULL AND (EARLIEST_COMPLIANT_SPECIMEN_DATE >= DATE_ADD(x.ADMIDATE, 14) AND (EARLIEST_COMPLIANT_SPECIMEN_DATE <= DISDATE OR DISDATE IS NULL)) THEN 1 ELSE 0 END AS HO_DEFINITE_HA,
# MAGIC   CASE WHEN EARLIEST_COMPLIANT_SPECIMEN_DATE IS NOT NULL AND (EARLIEST_COMPLIANT_SPECIMEN_DATE > DISDATE AND EARLIEST_COMPLIANT_SPECIMEN_DATE <= DATE_ADD(DISDATE, 14)) THEN 1 ELSE 0 END AS CO_POSSIBLE_HA
# MAGIC FROM global_temp.ccu029_01_filtered_spells_table_name x
# MAGIC LEFT JOIN (
# MAGIC   SELECT
# MAGIC     a.PERSON_ID_DEID,
# MAGIC     ADMIDATE,
# MAGIC     MIN(Specimen_Date) AS EARLIEST_COMPLIANT_SPECIMEN_DATE
# MAGIC   FROM global_temp.ccu029_01_filtered_spells_table_name a
# MAGIC   LEFT JOIN global_temp.ccu029_01_sgss_censored b
# MAGIC   ON a.PERSON_ID_DEID = b.PERSON_ID_DEID
# MAGIC   WHERE Specimen_Date >= DATE_ADD(ADMIDATE, -13)
# MAGIC   GROUP BY a.PERSON_ID_DEID, ADMIDATE
# MAGIC ) y
# MAGIC ON x.PERSON_ID_DEID = y.PERSON_ID_DEID AND x.ADMIDATE = y.ADMIDATE

# COMMAND ----------

hes_apc_filtered_tests = spark.sql(f"""
SELECT
  x.*,
  FIRST_SPECIMEN_DATE_BEFORE_ADMISSION,
  CASE WHEN FIRST_SPECIMEN_DATE_BEFORE_ADMISSION IS NOT NULL THEN 1 ELSE 0 END AS PREVIOUS_INFECTION,
  DATEDIFF(COALESCE(FIRST_SPECIMEN_DATE_BEFORE_ADMISSION, EARLIEST_COMPLIANT_SPECIMEN_DATE), x.ADMIDATE) AS FIRST_COVID_TEST_ADMISSION_DELTA,
  CASE WHEN DATEDIFF(FIRST_SPECIMEN_DATE_BEFORE_ADMISSION, EARLIEST_COMPLIANT_SPECIMEN_DATE) < 0 THEN 1 ELSE 0 END AS REINFECTION,
  CASE WHEN (
    (
      x.ADMIDATE BETWEEN LEAST(FIRST_SPECIMEN_DATE_BEFORE_ADMISSION, EARLIEST_COMPLIANT_SPECIMEN_DATE, x.ADMIDATE)
      AND DATE_ADD(LEAST(FIRST_SPECIMEN_DATE_BEFORE_ADMISSION, EARLIEST_COMPLIANT_SPECIMEN_DATE, x.ADMIDATE), {case_hospitalisation_window})
    )
    AND (LEAST(FIRST_SPECIMEN_DATE_BEFORE_ADMISSION, EARLIEST_COMPLIANT_SPECIMEN_DATE, x.ADMIDATE) <= '{infection_censoring_date}')
  ) THEN 1 ELSE 0 END AS ADMISSION_IN_WINDOW_FLAG
FROM global_temp.ccu029_01_hes_apc_filtered_tests x
LEFT JOIN (
  SELECT
    a.PERSON_ID_DEID,
    ADMIDATE,
    MIN(Specimen_Date) AS FIRST_SPECIMEN_DATE_BEFORE_ADMISSION
  FROM global_temp.ccu029_01_hes_apc_filtered_tests a
  LEFT JOIN global_temp.ccu029_01_sgss_censored b
  ON a.PERSON_ID_DEID = b.PERSON_ID_DEID
  WHERE Specimen_Date <= DATE_ADD(ADMIDATE, -14)
  GROUP BY a.PERSON_ID_DEID, ADMIDATE
) y
ON x.PERSON_ID_DEID = y.PERSON_ID_DEID AND x.ADMIDATE = y.ADMIDATE""")

if verbose:
  print(f"Creating `{output_table_name}` with study start date == {study_start}")

hes_apc_filtered_tests.createOrReplaceGlobalTempView(output_table_name)
drop_table(output_table_name)
create_table(output_table_name)
create_temp_table(output_table_name, "output_table_name")

optimise_table(output_table_name, "PERSON_ID_DEID")

table = spark.sql(f"SELECT * FROM dars_nic_391419_j3w9t_collab.{output_table_name}")
print(f"`{output_table_name}` has {table.count()} rows and {len(table.columns)} columns.")

# COMMAND ----------

if test:
  print("Ensuring that no rows are lost by adding tests...")
  n_pre_tests = spark.sql("SELECT COUNT(*) FROM global_temp.ccu029_01_filtered_spells_table_name").first()[0]
  n_tests = spark.sql("SELECT COUNT(*) FROM global_temp.ccu029_01_output_table_name").first()[0]
  print(n_pre_tests, n_tests)
  assert n_pre_tests == n_tests

# COMMAND ----------

