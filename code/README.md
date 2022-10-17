# Pipeline for cohort creation and analysis

The main code for all of the curation and analyses carried out as part of this project can be found in the `pipeline` folder. Each step is documented and annotated where appropriate with tests throughout to ensure expected behaviour. 

*Note that this code is for usage within the NHSD TRE, utilising the existing table and data structure.*

This `pipeline` folder is augmented with some additional code functions (in the `auxiliary` folder) that see repeated use within this pipeline or across our other analyses. For example, this includes functions to identify and extract UHCs given lookback data for an individual in a cohort. Finally, the `config` folder contains some environmental variables that can be set to ensure the reproducibility and status of our cohort, e.g. study dates and lookback periods etc.

To give a plain english overview of the pipeline, we:
1. Prepare HES APC
    * Ensure consistent characteristics and demographics amongst HES APC records that share a `PERSON_ID_DEID` (identifier) and `ADMIDATE` (admission date)
    * Filter to valid admissions within the study period
    * Collate the episode-level data in HES APC to admission-level, such that there is one row per full admission per person, with all of the codes occurring across its constituent episodes concatenated in line with the occurrence of ethese episodes
    * Join IMD data using LSOA
    * Join testing data from SGSS where a test falls within the defined window of an `ADMIDATE`, i.e. associate the first occurrence of a positive test that is within 14-days prior to admission and up to discharge, if one is present 
2. Append HES CC to the data resulting from (1) to identify ICU stays occurring within valid admissions and their characteristics
3. Type the admissions according to our definitions
4. Prepare SGSS
    * Follow a similar strategy as in (1) to ensure consistent characteristics and demographics across records associated with the same individual (per their `PERSON_ID_DEID`)
    * Subset to only first infection dates per person.
5. Combine the cleaned, first-infection subset of SGSS with the typed admissions to determine the nature of each individual's first SARS-CoV-2 related hospital admission / infection. This can be characterised by an admission that fulfils one of our phenotypes, or a positive test in SGSS. Where both occur together we associate them.
6. Subset to only those individuals that are under 18 at the time of their first infection, and indeed, those that have *had* a first infection at all within our study period.
7. Lookback across each individual's medical history via HES OP, HES APC, HES CC and GDPPR, collecting all relevant codes to then identify UHCs and other risk factors.
8. Continue to build the cohort by linking data from ONS' death dataset and vaccination records.
9. Finalise the cohort by defining any remaining derived variables.
