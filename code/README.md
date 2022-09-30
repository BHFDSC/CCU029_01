# Pipeline for cohort creation and analysis

The main code for all of the curation and analyses carried out as part of this project can be found in the `pipeline` folder. Each step is documented and annotated where appropriate with tests throughout to ensure expected behaviour. This code is for usage within the NHSD TRE, utilising the tables and data structures that they have in place.

This `pipeline` folder is augmented with some auxiliary code functions (in the `auxiliary` folder) that see repeated use within this pipeline or across our other analyses, for example functions to identify and extract UHCs given lookback data for an individual in a cohort. Whilst `config` contains some variables that can be set to ensure the reproducibility and status of our cohort, e.g. study dates and lookback periods etc.

<!-- To give a plain english overview of the pipeline, we:
1. Prepare HES APC
    * Ensure consistent characteristics and demographics amongst HES APC records that share a `PERSON_ID_DEID` (identifier) and `ADMIDATE` (admission date)
    * Filter to valid admissions within the study period
    * Collate the episode-level data in HES APC to admission-level, such that there is one row per full admission per person, with all of the codes occurring across its constituent episodes concatenated in line with the occurrence of ethese episodes
    * Join IMD data using LSOA
    * Join testing data from SGSS where a test falls within the defined window of an `ADMIDATE`, i.e. associate the first occurrence of a positive test that is within 14-days prior to admission and up to discharge, if one is present 
2. Append HES CC to the data resulting from (1) to identify ICU stays occurring within valid admissions and their characteristics
3. Type the admissions according to our definitions
4. Prepare SGSS
    * Take  -->