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
  tests_table_name = tests_output_table_name
  admissions_table_name = admissions_typed_output_table_name
  output_table_name = first_infections_output_table_name
except:
  raise ValueError("RUN TABLE_NAMES FIRST")

if verbose:
  print("Joining admission and first tests datasets in order to identify and link first infections as a base cohort...")

# COMMAND ----------

def unique_and_filter(sequence, to_remove):
  filtered_sequence = [x for x in sequence if x not in to_remove]
  seen = set()
  return [x for x in filtered_sequence if not (x in seen or seen.add(x))]

# Select all columns from both tables and coalesce the ones that are in both, preferring records from admissions, as DOB is higher resolution here
admissions_col_names = spark.sql(f"SELECT * FROM dars_nic_391419_j3w9t_collab.{admissions_table_name}").schema.names
tests_col_names = spark.sql(f"SELECT * FROM dars_nic_391419_j3w9t_collab.{tests_table_name}").schema.names

cols_to_include = ",\n  ".join([
  f"COALESCE(a.{col_name}, b.{col_name}) AS {col_name}" if col_name in tests_col_names and col_name in admissions_col_names else f"`{col_name}`"
  for col_name in unique_and_filter(admissions_col_names + tests_col_names, ["ADMISSION_VARIANT_PERIOD", "POSITIVE_TEST_SPECIMEN_VARIANT_PERIOD"])
 ])

if test:
  print(admissions_col_names)
  print(tests_col_names)
  print(cols_to_include)

# COMMAND ----------

# The below statement is very complicated, and some important decisions are made here that define how our cohort ends up looking...
# 
# 1. We apply a FULL OUTER JOIN to include (and link where relevent) ALL records across first positive tests AND all admissions, some records will be a test with no admission, and others will be an admission with no test.
#   a. Naturally we join on PERSON_ID_DEID to get all combinations of records for each person.
#   b. We refine this by saying that we only want to join admissions and tests together when:
#        - The test date is up to `case_hospitalisation_window` days prior to date of admission
#        - and as late as the discharge date of the admission, or the discharge date is NULL and the positive test comes at some point during the admission / within the 42 day window before it again
#        - i.e. we say that these tests are a "conversion" to a hospitalisation within our defined window and we wish to link these records and count them together for purposes of CHR calculation etc.
#      The nature of the FULL OUTER JOIN is such that if an admission and test do not pass this definition, the records will be included independently instead, as desired.
# 
# 2. We now have all of the valid links between first tests and admissions, per our window, but we have many records we are not interested in as well due to one side of the join encompassing ALL admissions.
#   a. For example we will have all the non-COVID admissions happening after our defined hospitalisations start date cut-off, such that if brazenly left in the data could wrongly identify any non-COVID admission as someones' first infection if it is the first event for the individual. As such we require that any non-COVID admission is removed unless the admission date is occurring between the date of the individual's first test and that test date plus the `case_hospitalisation_window` such that these non-COVID admissions can be observed for purposes of non-COVID CHR, but otherwise will never identify an infection in lieu of a first test or COVID admission should it occur later on
#   b. Given (a) we must now define the records that we DO want in our infections for consideration, alongside those carefully picked in (a), these are:
#        - Records without an admission associated, i.e. a first test on its own, there may be other admissions in the dataset which could define other infections to consider, but they do not necessarily occur within our prescribed window and thus should be treated as separate infections
#        - Records without a positive test date, i.e. a COVID infection defined solely by a COVID admission
#        - Records with both a test and a COVID admission, here we require that the admission started on or prior to the positive test date plus the case hospitalisation window length, otherwise a test on its own would define a separate admission anyway due to our join conditions in (1)
#   c. This leaves us with all of the potential infections we want to consider (not actually all of them, but all of the candidates for the FIRST infection most importantly)
# 
# 3. We now want to unify the paired records' dates to provide a consistent basis for calculating things like age and the variant period the infection event belongs to. Our goal is to take the date of the FIRST COVID EVIDENCE, which for admissions is whichever came first of a test or admission date, except for Nosocomial cases where the test always defines the infection date; for tests without a linked admission it is naturally the test date. When one date is NULL the other is chosen by default under this approach. There is one SQL-driven complexity that means we must coalesce the positive test date with the earliest compliant specimen date associated with nosocomial admissions as in some cases there is a nosocomial admission associated with a test OTHER than the first positive test. In these instances the infection date would be NULL unless we fall back to the test date initially associated with the admission. Clearly in all of these cases this nosocomial admission cannot be the first infection and thus we exclude it, but if we have a NULL INFECTION_DATE then the INFECTION_IDX ends up wrongly being 1 due to the nature of SQL's ASC sorting.
# 
# 4. Finally we have an infection date and can order all of the infections by this date (giving preference to ones with COVID admissions associated if there are multiple on the same day for an individual) and filter down to a base cohort of under 18s at time of infection by using AGE relative to infection and filtering to only take the first infection for each individual (see defintion of INFECTION_IDX window) where that first infection occurs in the study period up to the censoring date.

# COMMAND ----------

full_cohort = spark.sql(f"""
SELECT *
FROM (
  SELECT
    GREATEST(0, DATEDIFF(INFECTION_DATE, DATE_OF_BIRTH) / 365.25) AS AGE,
    DATEDIFF('{study_start}', DATE_OF_BIRTH) / 365.25 AS AGE_AT_STUDY_START,
    ROW_NUMBER() OVER (PARTITION BY PERSON_ID_DEID ORDER BY INFECTION_DATE ASC, CASE WHEN META_TYPE == 'COVID' THEN 1 WHEN META_TYPE == 'COVID Excluded' THEN 2 ELSE 3 END ASC, ADMIDATE ASC) AS INFECTION_IDX,
    *
  FROM (
    SELECT
      CASE WHEN META_TYPE == 'COVID' THEN 1 ELSE 0 END AS COVID_ADMISSION_IN_WINDOW,
      CASE WHEN ADMIDATE IS NOT NULL AND META_TYPE != 'COVID' THEN 1 ELSE 0 END AS NON_COVID_ADMISSION_IN_WINDOW,
      CASE WHEN ADMIDATE IS NOT NULL THEN 1 ELSE 0 END AS ADMISSION_IN_WINDOW,
      CASE
        WHEN META_TYPE == 'COVID' AND POSITIVE_TEST_SPECIMEN_DATE IS NULL THEN 'Admission Only'
        WHEN META_TYPE == 'COVID' AND POSITIVE_TEST_SPECIMEN_DATE IS NOT NULL THEN 'Admission and Test'
        WHEN POSITIVE_TEST_SPECIMEN_DATE IS NOT NULL THEN 'Test Only'
        ELSE 'Error'
      END AS INFECTION_SOURCE,
      CASE WHEN META_TYPE == 'COVID' THEN DAY_CASE ELSE NULL END AS COVID_DAY_CASE,
      CASE WHEN META_TYPE == 'COVID' THEN ICU ELSE 0 END AS COVID_ICU,
      CASE WHEN META_TYPE == 'COVID' THEN STILL_IN_HOSPITAL ELSE NULL END AS COVID_STILL_IN_HOSPITAL,
      CASE WHEN META_TYPE == 'COVID' THEN LENGTH_OF_STAY ELSE NULL END AS COVID_LENGTH_OF_STAY,
      CASE WHEN META_TYPE == 'COVID' THEN POSITIVE_COVID_TEST_INCLUSION ELSE NULL END AS COVID_POSITIVE_COVID_TEST_INCLUSION,
      CASE WHEN TYPE_OF_ADMISSION == 'Nosocomial' THEN COALESCE(POSITIVE_TEST_SPECIMEN_DATE, EARLIEST_COMPLIANT_SPECIMEN_DATE) ELSE LEAST(ADMIDATE, POSITIVE_TEST_SPECIMEN_DATE) END AS INFECTION_DATE,
      CASE WHEN META_TYPE == 'COVID' THEN ADMIDATE ELSE COALESCE(POSITIVE_TEST_SPECIMEN_DATE, EARLIEST_COMPLIANT_SPECIMEN_DATE, ADMIDATE) END AS LOOKBACK_DATE,
      {cols_to_include}
    FROM dars_nic_391419_j3w9t_collab.{admissions_table_name} a
    -- FROM (SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY PERSON_ID_DEID ORDER BY ADMIDATE ASC) AS ADMISSION_IDX FROM dars_nic_391419_j3w9t_collab.{admissions_table_name} WHERE META_TYPE == 'COVID') WHERE ADMISSION_IDX == 1) a
    FULL OUTER JOIN dars_nic_391419_j3w9t_collab.{tests_table_name} b
    ON 
      a.PERSON_ID_DEID = b.PERSON_ID_DEID
      AND (
        POSITIVE_TEST_SPECIMEN_DATE BETWEEN DATE_ADD(ADMIDATE, -{case_hospitalisation_window}) AND DISDATE
        OR (DISDATE IS NULL AND POSITIVE_TEST_SPECIMEN_DATE >= DATE_ADD(ADMIDATE, -{case_hospitalisation_window}))
      )
    WHERE
      (ADMIDATE IS NOT NULL AND META_TYPE == 'COVID' AND ADMIDATE <= DATE_ADD(POSITIVE_TEST_SPECIMEN_DATE, {case_hospitalisation_window}) AND POSITIVE_TEST_SPECIMEN_DATE IS NOT NULL)
      OR (ADMIDATE IS NOT NULL AND META_TYPE == 'COVID' AND POSITIVE_TEST_SPECIMEN_DATE IS NULL)
      OR (ADMIDATE IS NOT NULL AND META_TYPE != 'COVID' AND ADMIDATE BETWEEN POSITIVE_TEST_SPECIMEN_DATE AND DATE_ADD(POSITIVE_TEST_SPECIMEN_DATE, {case_hospitalisation_window}))
      OR (ADMIDATE IS NULL)
  )
)
WHERE
  AGE < 18
  AND INFECTION_IDX == 1
  AND INFECTION_DATE BETWEEN '{study_start}' AND '{infection_censoring_date}'""")

if verbose:
  print(f"Creating `{output_table_name}` with study start date == {study_start}")

full_cohort.drop("INFECTION_IDX").createOrReplaceGlobalTempView(output_table_name)
drop_table(output_table_name)
create_table(output_table_name)
optimise_table(output_table_name, 'PERSON_ID_DEID')

table = spark.sql(f"SELECT * FROM dars_nic_391419_j3w9t_collab.{output_table_name}")
print(f"`{output_table_name}` has {table.count()} rows and {len(table.columns)} columns.")

# COMMAND ----------

if test:
  display(spark.sql(f"SELECT SUM(CASE WHEN COVID_ICU == 1 THEN 1 ELSE 0 END) AS NUM_ICU, SUM(CASE WHEN COVID_DAY_CASE == 1 THEN 1 ELSE 0 END) AS NUM_DAY_CASE, SUM(CASE WHEN COVID_STILL_IN_HOSPITAL == 1 THEN 1 ELSE 0 END) AS NUM_STILL_IN_HOSPITAL FROM dars_nic_391419_j3w9t_collab.{output_table_name}"))

# COMMAND ----------

if test:
  display(spark.sql(f"SELECT COUNT(*), COUNT(DISTINCT PERSON_ID_DEID) FROM dars_nic_391419_j3w9t_collab.{output_table_name}"))

# COMMAND ----------

if test:
  display(spark.sql(f"SELECT INFECTION_SOURCE, COUNT(*) FROM dars_nic_391419_j3w9t_collab.{output_table_name} GROUP BY INFECTION_SOURCE"))

# COMMAND ----------

display(spark.sql(f"SELECT COUNT(*) AS COVID_ADMISSIONS_IN_COHORT FROM dars_nic_391419_j3w9t_collab.{output_table_name} WHERE COVID_ADMISSION_IN_WINDOW == 1"))

# COMMAND ----------

if test:
  display(spark.sql(f"SELECT COUNT(*) FROM dars_nic_391419_j3w9t_collab.{output_table_name} WHERE ADMISSION_IN_WINDOW == 1"))

# COMMAND ----------

if test:
  display(spark.sql(f"SELECT META_TYPE, COUNT(*) FROM dars_nic_391419_j3w9t_collab.{output_table_name} GROUP BY META_TYPE"))

# COMMAND ----------

if test:
  display(spark.sql(f"SELECT TYPE_OF_ADMISSION, META_TYPE, COUNT(*) FROM dars_nic_391419_j3w9t_collab.{output_table_name} WHERE TYPE_OF_ADMISSION NOT RLIKE 'Exclude' GROUP BY TYPE_OF_ADMISSION, META_TYPE ORDER BY META_TYPE, TYPE_OF_ADMISSION"))

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW ccu029_01_all_infections_with_excluded_covid_admissions AS
SELECT *
FROM (
  SELECT
    GREATEST(0, DATEDIFF(INFECTION_DATE, DATE_OF_BIRTH) / 365.25) AS AGE,
    DATEDIFF('{study_start}', DATE_OF_BIRTH) / 365.25 AS AGE_AT_STUDY_START,
    ROW_NUMBER() OVER (PARTITION BY PERSON_ID_DEID ORDER BY INFECTION_DATE ASC, CASE WHEN META_TYPE != 'Non-COVID' THEN 1 ELSE 2 END ASC, ADMIDATE ASC) AS INFECTION_IDX,
    *
  FROM (
    SELECT
      CASE WHEN META_TYPE != 'Non-COVID' THEN 1 ELSE 0 END AS COVID_ADMISSION_IN_WINDOW,
      CASE WHEN ADMIDATE IS NOT NULL AND META_TYPE == 'Non-COVID' THEN 1 ELSE 0 END AS NON_COVID_ADMISSION_IN_WINDOW,
      CASE WHEN ADMIDATE IS NOT NULL THEN 1 ELSE 0 END AS ADMISSION_IN_WINDOW,
      CASE
        WHEN META_TYPE != 'Non-COVID' AND POSITIVE_TEST_SPECIMEN_DATE IS NULL THEN 'Admission Only'
        WHEN META_TYPE != 'Non-COVID' AND POSITIVE_TEST_SPECIMEN_DATE IS NOT NULL THEN 'Admission and Test'
        WHEN POSITIVE_TEST_SPECIMEN_DATE IS NOT NULL THEN 'Test Only'
        ELSE 'Error'
      END AS INFECTION_SOURCE,
      CASE WHEN META_TYPE != 'Non-COVID' THEN DAY_CASE ELSE NULL END AS COVID_DAY_CASE,
      CASE WHEN META_TYPE != 'Non-COVID' THEN ICU ELSE 0 END AS COVID_ICU,
      CASE WHEN META_TYPE != 'Non-COVID' THEN STILL_IN_HOSPITAL ELSE NULL END AS COVID_STILL_IN_HOSPITAL,
      CASE WHEN META_TYPE != 'Non-COVID' THEN LENGTH_OF_STAY ELSE NULL END AS COVID_LENGTH_OF_STAY,
      CASE WHEN META_TYPE != 'Non-COVID' THEN POSITIVE_COVID_TEST_INCLUSION ELSE NULL END AS COVID_POSITIVE_COVID_TEST_INCLUSION,
      CASE WHEN TYPE_OF_ADMISSION == 'Nosocomial' THEN COALESCE(POSITIVE_TEST_SPECIMEN_DATE, EARLIEST_COMPLIANT_SPECIMEN_DATE) ELSE LEAST(ADMIDATE, POSITIVE_TEST_SPECIMEN_DATE) END AS INFECTION_DATE,
      CASE WHEN META_TYPE != 'Non-COVID' THEN ADMIDATE ELSE COALESCE(POSITIVE_TEST_SPECIMEN_DATE, EARLIEST_COMPLIANT_SPECIMEN_DATE, ADMIDATE) END AS LOOKBACK_DATE,
      {cols_to_include}
    FROM dars_nic_391419_j3w9t_collab.{admissions_table_name} a
    FULL OUTER JOIN dars_nic_391419_j3w9t_collab.{tests_table_name} b
    ON 
      a.PERSON_ID_DEID = b.PERSON_ID_DEID
      AND (
        POSITIVE_TEST_SPECIMEN_DATE BETWEEN DATE_ADD(ADMIDATE, -{case_hospitalisation_window}) AND DISDATE
        OR (DISDATE IS NULL AND POSITIVE_TEST_SPECIMEN_DATE >= DATE_ADD(ADMIDATE, -{case_hospitalisation_window}))
      )
    WHERE
      (ADMIDATE IS NOT NULL AND META_TYPE != 'Non-COVID' AND ADMIDATE <= DATE_ADD(POSITIVE_TEST_SPECIMEN_DATE, {case_hospitalisation_window}) AND POSITIVE_TEST_SPECIMEN_DATE IS NOT NULL)
      OR (ADMIDATE IS NOT NULL AND META_TYPE != 'Non-COVID' AND POSITIVE_TEST_SPECIMEN_DATE IS NULL)
      OR (ADMIDATE IS NOT NULL AND META_TYPE == 'Non-COVID' AND ADMIDATE BETWEEN POSITIVE_TEST_SPECIMEN_DATE AND DATE_ADD(POSITIVE_TEST_SPECIMEN_DATE, {case_hospitalisation_window}))
      OR (ADMIDATE IS NULL)
  )
)
WHERE
  AGE < 18
  AND INFECTION_IDX == 1
  AND INFECTION_DATE BETWEEN '{study_start}' AND '{infection_censoring_date}'""")

display(spark.sql(f"SELECT COUNT(*) AS COVID_ADMISSIONS_IN_COHORT_IF_WE_ALLOWED_NON_PRIMARY_OTHER_COVID_CODES FROM global_temp.ccu029_01_all_infections_with_excluded_covid_admissions WHERE COVID_ADMISSION_IN_WINDOW == 1"))

# COMMAND ----------

if test:
  display(spark.sql(f"SELECT META_TYPE, COUNT(*) FROM global_temp.ccu029_01_all_infections_with_excluded_covid_admissions GROUP BY META_TYPE ORDER BY META_TYPE"))

# COMMAND ----------

if test:
  display(spark.sql(f"SELECT TYPE_OF_ADMISSION, META_TYPE, COUNT(*) FROM global_temp.ccu029_01_all_infections_with_excluded_covid_admissions WHERE TYPE_OF_ADMISSION NOT RLIKE 'Other' GROUP BY TYPE_OF_ADMISSION, META_TYPE ORDER BY META_TYPE, TYPE_OF_ADMISSION"))

# COMMAND ----------

