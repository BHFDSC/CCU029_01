# Reference data

This folder contains a series of `.csv` files used for study definitions, lookups and reference data.

* [admission_type_definitions.csv](admission_type_definitions.csv)
    * Single `.csv` containing definitions of COVID-19 Hospital Admission Types
    * For reproduction outside the TRE see the [`/phenotypes/` folder](..\phenotypes\README.md)
* [CCACT_code_lookup.csv](CCACT_code_lookup.csv)
    * Lookup for `CCACTCODEn` Critical Care Activity Code fields within [HES CC](https://digital.nhs.uk/data-and-information/data-tools-and-services/data-services/hospital-episode-statistics/hospital-episode-statistics-data-dictionary)
* [dict_ethnic.csv](dict_ethnic.csv)
    * Lookup for ethnicity fields in both [HES APC](https://digital.nhs.uk/data-and-information/data-tools-and-services/data-services/hospital-episode-statistics/hospital-episode-statistics-data-dictionary) and GDPPR
* [ONS_prevalence.csv](ONS_prevalence.csv)
    * ONS Community Infection Survey data used in the top panel of Figure 1
* [uk90.csv](uk90.csv)
    * Reference ranges for age, BMI, height & weight for UK children
    * Used for calculation of z-scores and defining obesity