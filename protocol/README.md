<!-----

Yay, no errors, warnings, or alerts!

Conversion time: 1.498 seconds.


Using this Markdown file:

1. Paste this output into your source file.
2. See the notes and action items below regarding this conversion run.
3. Check the rendered output (headings, lists, code blocks, tables) for proper
   formatting and use a linkchecker before you publish this page.

Conversion notes:

* Docs to Markdown version 1.0β33
* Thu Sep 29 2022 10:40:59 GMT-0700 (PDT)
* Source doc: CCU029_01 Protocol
* Tables are currently converted to HTML tables.
----->



## CCU029_01


## Hospital admissions linked to SARS-CoV-2 infection in children


## Authors

Harrison Wilde, Chris Tomlinson, Bilal Mateen, Christina Pagel, Katherine Brown


## Version history


<table>
  <tr>
   <td><strong>Version</strong>
   </td>
   <td><strong>Date</strong>
   </td>
   <td><strong>Notes</strong>
   </td>
  </tr>
  <tr>
   <td>0.1
   </td>
   <td>08/02/2022
   </td>
   <td>Initial draft by authors
   </td>
  </tr>
</table>



## 


## Background

During the first two waves of the pandemic in the UK, the predominant focus of health professionals has been on managing the large number of sick adults and getting the very successful COVID-19 vaccination programme up and running. During this period, there was great relief that children were relatively spared from the direct impacts of COVID-19, and the main focus up to now has been on indirect impacts such as school closures and mental health issues.  

The direct health impacts of COVID-19 in children have been less well studied, although the low death rate has been emphasised (for example a recent paper estimated the case fatality rate of COVID-19 from the first two waves in the UK as 0.005%)[[1]](https://paperpile.com/c/npFJxx/PQEY). Nonetheless, mortality is an extreme health outcome, and we propose that a wider range of important health outcomes should also be considered, including severe disease due to COVID-19 leading to hospitalisation[[2,3]](https://paperpile.com/c/npFJxx/CyFV+e9Qc), critical illness and longer-term health problems in a subset of children[[4]](https://paperpile.com/c/npFJxx/8ocV). A study from the first two pandemic waves in England found that amongst paediatric hospital admissions, children with COVID-19 had higher odds of comorbidity and minority ethnic background than other non COVID-19 admissions[[5]](https://paperpile.com/c/npFJxx/f78F).  

It would be advantageous to undertake further analysis using the wider range of datasets available via the CVD-COVID-UK/COVID-IMPACT consortium, since this would enable consideration of additional important risk factors not previously studied such as child obesity.

This proposed new analysis is important because the American Association of Pediatrics in the USA is reporting rising hospitalisation in children since delta became dominant in the USA [[6]](https://paperpile.com/c/npFJxx/0ml7), especially amongst children of non-White ethnicity[[7]](https://paperpile.com/c/npFJxx/O7VZ), and a recent Public Health England report confirmed that the delta strain is more likely to lead to hospitalisation in unvaccinated patients of all ages[[8]](https://paperpile.com/c/npFJxx/GzoaS). 


## 


## Research aim and objectives

**Objective 1**


    Create and describe a classification of paediatric hospital admissions linked to SARS-CoV-2 infection as one of: a) primarily due to SARS-CoV-2 (including acute COVID-19 syndrome and paediatric inflammatory multisystem syndrome temporally associated with SARS-CoV-2 (PIMS-TS)); b) admissions where SARS-CoV-2 was likely to be on the causal pathway; c) admissions with incidental infection; or d) nosocomial infections.

**Objective 2**

** **


    Describe the admission demographics including UHCs (both as defined by the Joint Committee on Vaccination and Immunisation (JCVI) and a wider selection of non-acute paediatric UHCs identified by a panel of consultant paediatricians);

**Objective 3 **


## 
    Describe trends in first ascertained SARS-CoV-2 cases, hospital, and Intensive Care Unit (ICU) admissions, and CHRs, stratified by dominant variant eras (Original, Alpha, Delta, and Omicron).


## Data sources

For ascertaining COVID-19 infections and extracting relevant covariates and outcomes for the analyses, we will use the following data sources in the NHS Digital TRE for England:



* COVID-19 testing data - Health Security Agency (HSA)/Public Health England (PHE) Second Generation Surveillance System (SGSS)
* Primary care data - General Practice Extraction Service Extract for Pandemic Planning and Research (GDPPR)
* Secondary care data - Hospital Episode Statistics (HES) Admitted Patient Care (APC), Adult Critical Care (CC) and Outpatient (OP)
* Mortality - Office of National Statistics death registration records
* COVID-19 vaccination status
* Paediatric Intensive Care Audit Network (PICANet) - <span style="text-decoration:underline;">pending availability within the TRE</span>

Further population-level statistics used in analysis:



* COG-UK/Mutation Explorer[[9]](https://paperpile.com/c/npFJxx/iHw8a) used to infer likely variant based on population proportions


## Study design

Retrospective cohort study using linked electronic health records.


## Study population

Patients are included if they meet the following criteria:



* Valid and non-missing patient pseudoidentifier, age and sex
* Alive at study start date, or born during the study period
* Aged 0-17 years old at the time of first ascertained SARS-CoV-2 infection
* Located in England, as defined by the Lower-layer Super Output Area (LSOA) associated with their electronic health records


## Study dates



* Start date is 1st July 2020, when testing was well established in the community[[10]](https://paperpile.com/c/npFJxx/0M1L)
* End date is the last available episode start date in HES APC
* First infections, identified by positive tests, will be censored 6 weeks prior to study end date to capture related admissions and outcomes


## Exposures



* First ascertained SARS-CoV-2 infections
    * Identified based on either a first positive SARS-CoV-2 test in SGSS, 
    * Or a first SARS-CoV-2 related hospital admission as defined in the ‘Outcome’ section
* Where applicable, we will associate a child’s first positive test with their first SARS-CoV-2 related admission
    * If the test occurred between 6 weeks prior to the date of admission (the maximum reported time between an infection and PIMS-TS associated hospitalisation[[11]](https://paperpile.com/c/npFJxx/RFwh)) and the date of hospital discharge (for identification of nosocomial infections). 


## Outcomes


### Hospital admission types

Since SARS-CoV-2 infection is a new disease with emerging clinical characteristics in children, we will adopt broad inclusion criteria to identify SARS-CoV-2 related hospitalisations, before classifying admission types. 

We included first SARS-CoV-2 related hospitalisations where at least one of the following criteria was met:



1. primary cause for hospitalisation was an ICD-10 code in HES APC related to SARS-CoV-2 infection: U07.1, U07.2, U07.3, U07.420 (we refer to these as ‘Covid codes’); or a non-primary cause for hospitalisation was an ICD-10 code for acute COVID-19: U07.1 or U07.2. 
2. a primary or non-primary cause for admission in HES APC was an ICD-10 code used to identify PIMS-TS (introduced from May 2020): R65, M30.3, U07.520 (we refer to these as ‘PIMS-TS codes’) and there were no codes that indicate an alternative diagnosis
3. there was a positive SARS-CoV-2 test from up to 14 days before hospitalisation until the date of hospital discharge (we refer to this as ‘Positive test’). 

From amongst these SARS-CoV-2 related admissions, we will adopt a hierarchical approach to identify mutually exclusive hospital admission types as shown in detail below, using a combination of the ICD-10 codes listed as reason for admission and the SGSS testing data. For clinical reasons as detailed in Box 1 that were developed by consensus, SARS-CoV-2 related hospital admission types will be identified in the following order as: 



1. nosocomial admissions 
2. incidental admissions
3. PIMS-TS admissions 
4. admissions caused by acute SARS-CoV-2 infection
5. admissions where SARS-CoV-2 infection was not the primary cause, but likely contributed to needing hospitalisation. 

This process has resulted from consultation with a consultant paediatrician panel, all of whom have direct experience of caring for children hospitalised with SARS-CoV-2, (KB, HK, NP, MJ, PDP, PR) with each clinical code list agreed upon by at least two members.


<table>
  <tr>
   <td>
    <strong>Identified admission types in order of assignment</strong>
   </td>
   <td>
    <strong>Description of codes and methods for identifying each admission type</strong>
   </td>
  </tr>
  <tr>
   <td><strong>1) Nosocomial admissions</strong>
   </td>
   <td>
    Consistent with definitions used by NHS England<sup>21</sup>, we classified an admission as nosocomial if the first associated Positive test occurred between day 8 of hospitalisation and hospital discharge and there were no Covid codes (U07.1, U07.2) provided as a cause for hospitalisation before day 8 of the admission .
   </td>
  </tr>
  <tr>
   <td><strong>2)  Incidental Admissions  “Type C”</strong>
   </td>
   <td>
    We classified incidental admissions before those directly caused by or contributed to by SARS-CoV-2 infection, to remove the possibility that this type of admission could be misclassified. These are admissions where SARS-CoV-2 is not the cause but is coincidental due to community transmission. Candidate reasons were identified in the International Severe Acute Respiratory and emerging Infection Consortium (ISARIC) prospective study of COVID-19 in children, such as trauma, poisoning, or elective surgery.<sup>22</sup> We identified and included a wider range of relevant primary reasons for admission than ISARIC, by iterative clinical review of codes in the admissions dataset (Supplementary Materials Table A: mental health disorders, eye conditions, dental conditions, injuries, trauma, assault, self-harm, poisoning, surgical problems such as those affecting bowel or testis, certain pregnancy related conditions). Incidental admissions all had codes U07.1 or U07.2 as a non-primary reason for admission and / or a Positive test before day 8 of the admission.
   </td>
  </tr>
  <tr>
   <td><strong>3)  Paediatric Inflammatory Multisystem Syndrome with a temporal association with SARS-COV-2 (PIMS-TS) Admissions</strong>
   </td>
   <td>
    We classified an admission as PIMS-TS if a reason for hospitalisation was a PIMS-TS Code (R65, M30.3, U07.5) ,<sup>20</sup> but none of the pre-specified exclude codes (Supplementary Materials Table A) indicating an alternative diagnosis of sepsis or major bacterial infection were present. Patients with PIMS-TS admission type could also have a Covid code as a reason for admission and may or may not have a Positive test as it has previously been demonstrated that the majority are PCR negative.<sup>23-25</sup> After clinical review of the ICD-10 codes listed as the reasons for admission amongst children who had a PIMS-TS code as a non-primary reason for admission, these were included because (after applying the listed exclusion codes) these codes were consistent with a diagnosis of PIMS-TS.
   </td>
  </tr>
  <tr>
   <td>
    <strong>4)   Admissions directly caused by SARS-CoV-2 infection</strong>
<p>

    <strong>“Type A”</strong>
<p>

    <strong> </strong>
   </td>
   <td>
    Next, we classified admissions where the main reason was acute SARS-CoV-2 infection (Type A) as follows:
<p>

    Type A1) A primary reason for admission was a Covid Code ((U07.1, U07.2, U07.3, U07.4).<sup>20</sup>
<p>

    Type A2) A primary reason for admission was a sign, symptom, or condition/presentation consistent with an acute SARS-CoV-2 infection (and did not definitively indicate an alternative diagnosis) and a non-primary reason for admission was a code for acute covid (U07.1 or U07.2). The candidate list of signs, symptoms or conditions of SARS-CoV-2 infection was identified from prospective studies including ISARIC and international studies <sup>22 26-29</sup> as: unspecified viral infections, viral conjunctivitis, volume depletion, shock, acidosis, otitis media, croup, non-specific bronchiolitis, cough, fever, vomiting, diarrhoea, myalgia, headache, , arrhythmias, tonsilitis, pharyngitis, laryngitis (Supplementary Materials Table A).
<p>

    These admissions may or may not have been linked to a Positive test.
   </td>
  </tr>
  <tr>
   <td>
    <strong>5)  Admissions with SARS-CoV-2 infection as likely contributor</strong>
<p>

    <strong>“Type B”</strong>
<p>

    <strong> </strong>
   </td>
   <td>
    Finally, we assigned admissions where SARS-CoV-2 infection was likely to be on the causal pathway, albeit not the primary cause of the admission (Type B). These hospitalisations may or may not have been linked to a Positive test, and all had a code for acute covid (U07.1 or U07.2) as a non-primary reason for hospital admission, combined with one of the following primary reasons for admission that were all deemed relevant based on published reports <sup>30 31</sup> <sup>32</sup> <sup>22 26</sup> or, given the emerging nature of the topic, based on expert clinical experience (Supplementary Materials Table A):
<p>

    Type B1) A condition known to co-occur with SARS-CoV-2 (co or secondary infections such as respiratory syncytial virus, adenovirus, staphylococcal pneumonia, streptococcal pneumonia) <sup>30 31</sup> <sup>32</sup>;  a condition that has been clinically linked to SARS-CoV-2 infection in children <sup>31 32</sup> (type 1 diabetes mellitus, status epilepticus or febrile seizures); or a small number of treatments that could be linked to SARS-CoV-2 infection (isolation in cubicle for droplet precautions).
<p>

    Type B2) A pre-existing or newly diagnosed condition associated with higher risk of severe illness from COVID-19 <sup>22 26</sup>(conditions treated with immunosuppression, any cancer, neurodevelopmental conditions that may affect breathing, neonatal conditions such as poor feeding, respiratory diseases such as asthma).
   </td>
  </tr>
</table>



### Secondary Outcomes


### **Intensive Care Unit admission**



* The presence of a patient episode in HES CC within the admission spell


### **Mortality**



* Via linkage to ONS deaths registry


## Covariates



* **Date of birth, sex and ethnicity** will be derived from the ‘patient skinny record’ which aggregates data across multiple sources including GDPPR and HES APC
* **Age **will be measured at the date of first COVID-19 infection
* **Socioeconomic deprivation** information will be derived using the 2011 Lower-layer Super Output Areas (LSOA) from GDPPR to index the 2019 English indices of deprivation[[12]](https://paperpile.com/c/npFJxx/1zkFI). Index of Multiple Deprivation (IMD) will then be mapped to fifths (1 = most deprived, 5 = least)
* **Vaccination status** will be defined as below when appearing >= 14 days prior to COVID-19 infection, accounting for the time taken to seroconvert and in line with other studies[[13]](https://paperpile.com/c/npFJxx/mOn11)
    * Dose 1
    * Dose 2
    * Dose 3. Third primary doses and booster doses will be aggregated within ‘dose 3’ as the SNOMED-CT codes for third primary dose appear poorly utilised.
    * The absence of an entry in the vaccine status database, or the administration of a first dose &lt;14 days before a COVID-19 infection, will be interpreted as unvaccinated
* **COVID-19 variant** at <span style="text-decoration:underline;">the time of first COVID-19 infection</span> will be approximated by using the method of CCU029, using time eras to assign periods when each SARS-CoV-2 variant was dominant in the English population[[9]](https://paperpile.com/c/npFJxx/iHw8a) as follows:
    * Original variant: 1st July 2020 to 5th December 2020
    * Alpha variant: 3rd January2020 to 1st May 2021
    * Delta variant 30th May 2021 to 11th December 2021 
    * Omicron 26th December to study end (Omicron sub-lineages will not be reported)
    * To avoid periods where there is not a single dominant variant, we will use “inter-variant” periods between each of these variant windows
* **Underlying Health Conditions ‘UHCs’**
    * We will consider conditions listed in ICD-10 (2019) and a scheme for identifying comorbidities in HES by Hardelid et al [[14]](https://paperpile.com/c/npFJxx/S5b7) specific to children and young people, for candidate UHC. 
        * For young children, a first hospitalisation might be the only source of UHC information, hence we will consider any mention of congenital or chronic UHC in the EHR from birth to first ascertained infection, inclusive (this differs to adult methods where only prior history is considered). 
        * In acquired or reversible UHC (for example, cancer, conditions of prematurity) as undertaken in prior studies based on the Hardelid paediatric UHCs we will limit these to 5 years prior to first ascertained infection[[14]](https://paperpile.com/c/npFJxx/S5b7). 
    * We will seek to reproduce the specific subset of conditions identified by JCVI and listed in the Green Book as linked to higher risk of severe COVID-19[[15]](https://paperpile.com/c/npFJxx/iOl3)
        * We will additionally identify a more expansive list clinically-judged to be chronic congenital or acquired conditions in children, excluding acute paediatric illnesses. 
    * We will use relevant ICD-10 codes from GDPPR (primary care), HES APC and HES OP (secondary care), dated up to and including the admission of interest. 
        * In the case of GDPPR, NHS-D Technology Reference Update Distribution (TRUD) cross-maps will be used to transform SNOMED-CT codes into ICD-10 codes[[16]](https://paperpile.com/c/npFJxx/SfC5)
        * All codelists will be informed by iterative clinician review and exploratory data analysis
        * To avoid inclusion of acute conditions caused by SARS-CoV-2 as UHC, clinicians will review all the diagnoses present in the study codelists and exclude these. 


## Statistical analysis



* Descriptive statistics of covariates including counts, percentages, mean/median, SD/IQR - as appropriate
* Null hypothesis significance testing will be used to compare proportions of demographics, comorbidities and outcomes between groups


## 


# References


    1. 	[Smith C, Odd D, Harwood R, Ward J, Linney M, Clark M, et al. Deaths in Children and Young People in England following SARS-CoV-2 infection during the first pandemic year: a national study using linked mandatory child death reporting data. bioRxiv. medRxiv; 2021. doi:10.1101/2021.07.07.21259779](http://paperpile.com/b/npFJxx/PQEY)


    2. 	[Swann OV, Holden KA, Turtle L, Pollock L, Fairfield CJ, Drake TM, et al. Clinical characteristics of children and young people admitted to hospital with covid-19 in United Kingdom: prospective multicentre observational cohort study. BMJ. 2020;370: m3249.](http://paperpile.com/b/npFJxx/CyFV)


    3. 	[Issitt RW, Booth J, Bryant WA, Spiridou A, Taylor AM, du Pré P, et al. Children with COVID-19 at a specialist centre: initial experience and outcome. Lancet Child Adolesc Health. 2020;4: e30–e31.](http://paperpile.com/b/npFJxx/e9Qc)


    4. 	[Osmanov IM, Spiridonova E, Bobkova P, Gamirova A, Shikhaleva A, Andreeva M, et al. Risk factors for post-COVID-19 condition in previously hospitalised children using the ISARIC Global follow-up protocol: a prospective cohort study. Eur Respir J. 2022;59. doi:10.1183/13993003.01341-2021](http://paperpile.com/b/npFJxx/8ocV)


    5. 	[Ward JL, Harwood R, Smith C, Kenny S, Clark M, Davis PJ, et al. Risk factors for intensive care admission and death amongst children and young people admitted to hospital with COVID-19 and PIMS-TS in England during the first pandemic year. bioRxiv. medRxiv; 2021. doi:10.1101/2021.07.01.21259785](http://paperpile.com/b/npFJxx/f78F)


    6. 	[Sisk B. Children and COVID-19: State Data Report-A joint report from the American Academy of Pediatrics and the Children’s Hospital Association. 2020. Available: https://policycommons.net/artifacts/2097479/children-and-covid-19/2852777/](http://paperpile.com/b/npFJxx/0ml7)


    7. 	[[No title]. [cited 29 Sep 2022]. Available: https://www.cdc.gov/coronavirus/2019-ncov/community/health-equity/racial-ethnic-disparities/disparities-hospitalization.html](http://paperpile.com/b/npFJxx/O7VZ)


    8. 	[Twohig KA, Nyberg T, Zaidi A, Thelwall S, Sinnathamby MA, Aliabadi S, et al. Hospital admission and emergency care attendance risk for SARS-CoV-2 delta (B.1.617.2) compared with alpha (B.1.1.7) variants of concern: a cohort study. Lancet Infect Dis. 2022;22: 35–42.](http://paperpile.com/b/npFJxx/GzoaS)


    9. 	[COG-UK/Mutation Explorer. [cited 19 Aug 2022]. Available: https://sars2.cvr.gla.ac.uk/cog-uk/](http://paperpile.com/b/npFJxx/iHw8a)


    10. 	[England NHS. UK Gov Covid-19 Testing Data London UK: UK Government 2022. [cited 29 Sep 2022]. Available: https://coronavirus.data.gov.uk/details/testing?areaType=nation&areaName=England](http://paperpile.com/b/npFJxx/0M1L)


    11. 	[Nygaard U, Holm M, Hartling UB, Glenthøj J, Schmidt LS, Nordly SB, et al. Incidence and clinical phenotype of multisystem inflammatory syndrome in children after infection with the SARS-CoV-2 delta variant by vaccination status: a Danish nationwide prospective cohort study. Lancet Child Adolesc Health. 2022;6: 459–465.](http://paperpile.com/b/npFJxx/RFwh)


    12. 	[English indices of deprivation 2019. In: GOV.UK [Internet]. [cited 21 Apr 2022]. Available: https://www.gov.uk/government/statistics/english-indices-of-deprivation-2019](http://paperpile.com/b/npFJxx/1zkFI)


    13. 	[Dagan N, Barda N, Kepten E, Miron O, Perchik S, Katz MA, et al. BNT162b2 mRNA Covid-19 Vaccine in a Nationwide Mass Vaccination Setting. N Engl J Med. 2021;384: 1412–1423.](http://paperpile.com/b/npFJxx/mOn11)


    14. 	[Wijlaars LPMM, Gilbert R, Hardelid P. Chronic conditions in children and young people: learning from administrative data. Arch Dis Child. 2016;101: 881–885.](http://paperpile.com/b/npFJxx/S5b7)


    15. 	[Agency UHS. Coronavirus (COVID-19) vaccination information for public health professionals. The Green Book 2022.](http://paperpile.com/b/npFJxx/iOl3)


    16. 	[Home - TRUD. [cited 29 Sep 2022]. Available: https://isd.digital.nhs.uk/trud/user/guest/group/0/home](http://paperpile.com/b/npFJxx/SfC5)
