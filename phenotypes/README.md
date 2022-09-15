# COVID-19 phenotypes

## Study population inclusion criteria

All *first* hospital admissions occurring amongst *first* infections in our cohort as defined where the admission date is in the study era ()and there is one of:  

- a) `U071`, `U072` codes occurring in any position in their diagnosis (acute COVID-19 codes)  

- b) `U073`, `U074` codes occurring in a primary position only (the other two SARS-CoV-2 infection related codes)  

- c) a PIMS-TS code (`R65`, `M303`, `U075`) alongside none of the exclude codes (see PIMS-TS row) after the date PIMS-TS was defined (May 2020)   

- d) A positive test for COVID-19 14 days before hospitalisation up to the date of discharge.   

This description defines the study population with a SARS-CoV-2 related admission.

## COVID-19 admission types
In this work we classify COVID-19 admissions into the following types:
> * **Nosocomial**: First instance of a SARS-CoV-2 related code / positive test on day 8 or later of admission  
> * **Type C**: *Incidental* cases where there is a primary diagnosis which appears unrelated to SARS-CoV-2
> * **PIMS-TS**: All cases, with sepsis explicitly excluded due to code overlap
> * **Type A1**: SARS-CoV-2 infection is listed as a primary reason for hospital admission
> * **Type A2**: A COVID-19 symptom or typical/plausible presentations of COVID-19 is a primary reason for admission, alongside the presence of other evidence of SARS-CoV-2 infection via either a code or test
> * **Type B1**: Acute conditions linked to COVID-19 or that are known to co-occur with COVID-19
> * **Type B2**: Underlying health conditions that are known to make patients sicker with COVID-19 or are conditions that make hospitalisation more likely


## COVID-19 admission types codelists

Machine-readable codelists are available within the `codelists/` folder as `.csv` files.  
 
* [`covid-19_type_c.csv`](codelists/covid-19_type_c.csv)
* [`covid-19_pims-ts.csv`](codelists/covid-19_pims-ts.csv)
* [`covid-19_type_a1.csv`](codelists/covid-19_type_a1.csv)
* [`covid-19_type_a3.csv`](codelists/covid-19_type_a2.csv)
* [`covid-19_type_b1.csv`](codelists/covid-19_type_b1.csv)
* [`covid-19_type_b2.csv`](codelists/covid-19_type_b2.csv)


💡 Users working within the NHS Digital Trusted Research Environment (TRE) should use the associated Databricks notebooks to implement these phenotypes in a more efficient manner!

**Schema**:
* `Phenotype` the same as the file-name  
* `Terminology` ICD-10 for all codes  
* `Code` the ICD-10 code  
  * ❗ Note some of the codes present are *partial* ICD-10 codes.
    * e.g. `E7` which includes codes spanning `E70` ('*Disorders of aromatic amino-acid metabolism*') to `E79.9` ('*Disorder of purine and pyrimidine metabolism, unspecified*')
    * This method was adopted to reduce the burden of specification on clinicians generating codelists.
    * These phenotypes are applied using partial string matching.
* `Description` is the description of the ICD-10 code
    * ❗ *partial* ICD-10 codes don't have an associated description. 
* `Position` denotes whether the code is `Primary` (i.e. `DIAG_4_01` in HES APC) or Non-Primary (i.e. `DIAG_4_02`-`DIAG_4_20`  in HES APC)  
* `Filter` denotes whether the presence of the code determines inclusion or exclusion in the phenotype (`Include`/`Exclude`, respectively)

<br>

## COVID-19 admission types phenotyping algorithm

❗ Notes on applying the phenotyping algorithm:

* The following typing algorithm is applied **on the above study population**
* This is a *hierarchical* phenotyping algorithm and therefore must be applied in the order listed below ('Typing hierarchy order')
* The population remaining at Step 8 is excluded

<br>


<table style="undefined;table-layout: fixed; width: 2477px">
<colgroup>
<col style="width: 404.005682px">
<col style="width: 93.005682px">
<col style="width: 421.005682px">
<col style="width: 755.005682px">
<col style="width: 155.005682px">
<col style="width: 150.005682px">
<col style="width: 292.005682px">
<col style="width: 207.005682px">
</colgroup>
<thead>
  <tr>
    <th>Population considered for inclusion</th>
    <th>Typing hierarchy <br>order</th>
    <th>Group</th>
    <th>Include Primary Codes</th>
    <th>Exclude Primary Codes</th>
    <th>Include Non-Primary Codes</th>
    <th>Exclude Non-Primary Codes</th>
    <th>Nosocomial (First instance of a SARS-CoV-2 related code / positive test on day 8 or later of admission)</th>
  </tr>
</thead>
<tbody>
  <tr>
    <td>Study population (as described above)</td>
    <td>1</td>
    <td>Nosocomial: As defined by Healthcare-associated Covid-19 in England: A national data linkage study. Bhattacharya, Alex et al. Journal of Infection, Volume 83, Issue 5, 565 – 572</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td>TRUE</td>
  </tr>
  <tr>
    <td>The above population minus Nosocomial admissions.</td>
    <td>2</td>
    <td>Type C: Incidental cases where there is a primary diagnosis which appears unrelated to SARS-CoV-2 </td>
    <td>A00,A01,A02,A03,A04,A05,A06,A07,A080,A081,A082,A15,A16,A17,A18,A19,A2,A3,A5,A6,  <br>B00,B01,B02,B03,B04,B05,B06,B07,B08,B1,B2,B3,B4,B5,B6,B7,B8,<br>D50,D51,D52,D53,D73,<br>E0,E2,E3,E5,<br>F,<br>G0,G5,G6,G9,<br>H,<br>I0,I60,I61,I62,I7,I8,<br>J6,J95,<br>K0,K1,K20,K25,K26,K27,K28,K29,K3,K4,K55,K56,K590,K58,K59,K6,K8,K9,<br>L0,L2,L3,L4,L5,L6,L72,L73,L8,L9,<br>M0,M1,M2,M4,M5,M6,M7,M8,M9,<br>N141,N2,N3,N39,N4,N5,N6,N7,N8,N9,<br>O,<br>P1,P20,P21,P50,P51,P52,P53,P54,P55,P56,P57,P58,P59,P6,P70,P71,P72,P75,P76,P77,P78,P8,P90,P91,P94,P95,P96,<br>Q1,Q4,Q5,Q6,Q7,Q8,<br>R12,R14,R15,R19,R20,R22,R23,R29,R30,R31,R32,R33,R35,R39,R45,R46,R80,R81,R82,R93,<br>U076,U82,U83,<br>S,<br>T,<br>V,<br>W,<br>X,<br>Y,<br>Z</td>
    <td>B20,B21,B22,B23,B24,<br>F7,F8,<br>H669,<br>K44,<br>Y4,Y5,<br>Z038,Z039,<br>U075,U109,M303,R65</td>
    <td></td>
    <td>U075,U109,M303,R65</td>
    <td></td>
  </tr>
  <tr>
    <td>The above population minus Incidental admissions.</td>
    <td>3</td>
    <td>PIMS-TS: All cases, with sepsis explicitly excluded due to code overlap</td>
    <td>U075,U109,M303,R65</td>
    <td>A01,A02,A03,A04,A05,A37,A38,A39,A40,A41,<br>B95</td>
    <td>U075,U109,M303,R65</td>
    <td>A01,A02,A03,A04,A05,A37,A38,A39,A40,A41,<br>B95</td>
    <td></td>
  </tr>
  <tr>
    <td>The above population minus PIMS-TS admissions.</td>
    <td>4</td>
    <td>Type A1: SARS-CoV-2 infection is listed as a primary reason for hospital admission</td>
    <td>U071,U072,U073,U074</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
  </tr>
  <tr>
    <td>The above population minus admissions where a SARS-CoV-2 related code is in a primary position.</td>
    <td>5</td>
    <td>Type A2: A Covid-19 symptom or typical/plausible presentations of Covid-19 is a primary reason for admission, alongside the presence of other evidence of SARS-CoV-2 infection via either a code or test</td>
    <td>A084,A083,A085,A090,A099,A419,<br>B348,B309,B338,B349,B972,B99,<br>D762,<br>E86,E87,<br>H669,<br>I1254,I126,I1471,I254,I26,I288,I30,I31,I32,I33,I40,I41,I42,I44,I45,I46,I47,I48,I49,I50,I51,I63,I65,I66,I67,I880,I9,<br>J00,J01,J04,J05,J06,J18,J22,J40,J80,J81,J83,J90,J93,J96,J98,<br>K297,K529,<br>M255,M791,M796,<br>N179,<br>P928,P25,P92,<br>R0,R10,R11,R13,R17,R21,R25,R26,R27,R29,R34,R40,R41,R42,R43,R44,R47,R5,R6,R768,R845,R89,R90,R93,R94,<br>U049,U070</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
  </tr>
  <tr>
    <td>The above population minus Type A2 admissions.</td>
    <td>6</td>
    <td>Type B1: Acute conditions linked to Covid-19 or that are known to co-occur with Covid-19</td>
    <td>A40,A41,A483,A49,A818,A870,A878,A89,A858,A86,A879,<br>B09,B34,B95,B96,B97,<br>E10,E11,E14,E16,<br>G4,<br>J02,J03,J09,J10,J11,J12,J13,J14,J15,J16,J17,J20,J21,J3,J85,J86,<br>K859,<br>P22,P23,P24,P26,P27,P28,P29,P3,P74,<br>R70,R71,R72,R73,R74,R79,<br>Z038,Z039,<br>U075,U109,M303,R65</td>
    <td></td>
    <td>U075,U109,M303,R65</td>
    <td></td>
    <td></td>
  </tr>
  <tr>
    <td>The above population minus Type B1 admissions.</td>
    <td>7</td>
    <td>Type B2: Underlying health conditions that are known to make patients sicker with Covid-19 or are conditions that make hospitalisation more likely</td>
    <td>B20,B21,B22,B24,<br>D3,<br>C,<br>D0,D1,D2,D4,D55,D56,D57,D58,D59,D6,D70,D71,D72,D730,D731,D761,D76,D8,<br>E10,E11,E12,E13,E14,E20,E21,E22,E23,E24,E25,E26,E27,E6,E7,E80,E81,E82,E83,E84,E850,E88,E89,<br>F7,F8,<br>G1,G2,G3,G7,G8,<br>I10,I11,I12,I13,I15,I27,I34,I35,I36,I37,I42,I43,I675,<br>J380,J386,J41,J42,J43,J44,J45,J46,J47,J82,J84,J99,<br>K21,K22,K44,K50,K51,K52,K71,K720,K721,K740,K741,K744,K745,K746,K75,K76,K9,<br>M3,<br>N0,N10,N11,N12,N13,N15,N18,<br>P0,<br>Q0,Q2,Q3,Q60,Q61,Q9,<br>R161</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
  </tr>
  <tr>
    <td>The above population minus Type B2 admissions.</td>
    <td>8</td>
    <td>Exclude: Any remaining admissions fall out of our types and thus may not be identifiably SARS-CoV-2 related; we exclude them all</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
  </tr>
</tbody>
</table>

<br>
<br>
<br>  

# Underlying Health Conditions (UHCs)

## UHC codelists

Machine-readable codelists are available within the `codelists/` folder as `.csv` files.  
 
* [`uhc_blood_disorders_and_immune_deficiencies.csv`](codelists/uhc_blood_disorders_and_immune_deficiencies.csv)
* [`uhc_cancer_and_neoplasms.csv`](codelists/uhc_cancer_and_neoplasms.csv)
* [`uhc_endocrine_conditions.csv`](codelists/uhc_endocrine_conditions.csv)
* [`uhc_neurological_and_developmental_conditions.csv`](codelists/uhc_neurological_and_developmental_conditions.csv)
* [`uhc_respiratory_conditions.csv`](codelists/uhc_respiratory_conditions.csv)
* [`uhc_congenital_heart_disease_and_hypertension_and_acquired_heart_disease.csv`](codelists/uhc_congenital_heart_disease_and_hypertension_and_acquired_heart_disease.csv)
* [`uhc_digestive_and_liver_conditions.csv`](codelists/uhc_digestive_and_liver_conditions.csv)
* [`uhc_muscle_and_skin_and_arthritis.csv`](codelists/uhc_muscle_and_skin_and_arthritis.csv)
* [`uhc_renal_and_genitourinary_conditions.csv`](codelists/uhc_renal_and_genitourinary_conditions.csv)
* [`uhc_prematurity_and_low_birth_weight.csv`](codelists/uhc_prematurity_and_low_birth_weight.csv)
* [`uhc_obesity.csv`](codelists/uhc_obesity.csv)
* [`uhc_pregnancy.csv`](codelists/uhc_pregnancy.csv)



💡 Users working within the NHS Digital Trusted Research Environment (TRE) should use the associated Databricks notebooks to implement these phenotypes in a more efficient manner!

**Schema**:
* `Phenotype` the same as the file-name  
* `Terminology` ICD-10 for all codes  
* `Code` the ICD-10 code  
  * ❗ Note some of the codes present are *partial* ICD-10 codes.
    * e.g. `E7` which includes codes spanning `E70` ('*Disorders of aromatic amino-acid metabolism*') to `E79.9` ('*Disorder of purine and pyrimidine metabolism, unspecified*')
    * This method was adopted to reduce the burden of specification on clinicians generating codelists.
    * These phenotypes are applied using partial string matching.
* `Description` is the description of the ICD-10 code
    * ❗ *partial* ICD-10 codes don't have an associated description. 
* `Filter` denotes whether the presence of the code determines inclusion or exclusion in the phenotype (`Include`/`Filter`, respectively)
  * Note the term `Filter` is used instead of Exclusion as the presence of a filter code does not exclude the patient having a phenotype, *if* they have another inclusion code
* `Lookback` denotes whether the previous time window over which a phenotype is valid
  * E.g. '9 months' for pregnancy


## UHC Phenotyping algorithm

* Obesity (Note here BMI values and Weight-for-age / BMI-for-age Z-scores may imply   this UHC)
* Pregnancy (Note here the standard approach does not apply, we only count pregnancy  codes occurring in the previous 9 months OR within the admission if one is   present)


<table style="undefined;table-layout: fixed; width: 1552px">
<colgroup>
<col style="width: 283.005682px">
<col style="width: 512.005682px">
<col style="width: 151.005682px">
<col style="width: 469.005682px">
<col style="width: 137.005682px">
</colgroup>
<thead>
  <tr>
    <th>UHC Name</th>
    <th>Include Codes</th>
    <th>Include Codes <br>(5 Year Lookback)</th>
    <th>Include Code <br>Chapter Filters</th>
    <th>Include Code <br>Chapter Filters   <br>(5 Year Lookback)</th>
  </tr>
</thead>
<tbody>
  <tr>
    <td>Blood Disorders and Immune Deficiencies</td>
    <td>B2,<br>D55,D56,D57,D58,D60,D61,D64,D66,D67,D680,D681,D682,D71,D730,D731,D8</td>
    <td></td>
    <td>B25,B26,B27,B28,B29,<br>D649</td>
    <td></td>
  </tr>
  <tr>
    <td>Cancer and Neoplasms</td>
    <td></td>
    <td>C,D0,D4</td>
    <td></td>
    <td>D473</td>
  </tr>
  <tr>
    <td>Endocrine Conditions</td>
    <td>E0,E1,E2,E3,E4,E7,E8</td>
    <td></td>
    <td>E231,E232,E86,E87,E162</td>
    <td></td>
  </tr>
  <tr>
    <td>Neurological and Developmental Conditions</td>
    <td>F7,F8,F9,<br>G1,G2,G3,G4,G5,G6,G7,G8,G9,<br>H17,H18,H19,H26,H33,H35,H40,H42,H46,H47,H48,H54,H90,H91,<br>I673,I675,I69,<br>P21,<br>Q0,Q1,Q9,<br>R62</td>
    <td></td>
    <td>F93,F94,F98,F99,<br>G25,G43,G44,G45,G46,G92,G93,G96,G97,G98,<br>H471</td>
    <td></td>
  </tr>
  <tr>
    <td>Respiratory Conditions</td>
    <td>E84,<br>J380,J386,J41,J42,J43,J44,J45,J46,J47,J82,J84,J99,<br>Q30,Q31,Q32,Q33,Q34</td>
    <td>P27</td>
    <td></td>
    <td></td>
  </tr>
  <tr>
    <td>Congenital Heart Disease and Hypertension and Acquired Heart disease</td>
    <td>I05,I06,I07,I08,I10,I11,I12,I13,I14,I15,I21,I22,I27,I28,I34,I35,I36,I37,I42,I43,I44,<br>Q2</td>
    <td></td>
    <td>Q245</td>
    <td></td>
  </tr>
  <tr>
    <td>Digestive and Liver Conditions</td>
    <td>K44,K50,K51,K52,K71,K721,K740,K741,K744,K745,K75,K754,K90,<br>Q35,Q36,Q37,Q39,Q4</td>
    <td>K21,K22</td>
    <td>K528,K529</td>
    <td></td>
  </tr>
  <tr>
    <td>Muscle and Skin and Arthritis</td>
    <td>L1,L4,L85,L93,L94,<br>M0,M1,M3,M41,M42,M43,<br>Q65,Q66,Q67,Q68,Q69,Q7,Q8</td>
    <td></td>
    <td>M01,M02,M03,M04,M303,<br>Q668,Q690,Q691,Q692,Q699,Q700,Q701,Q702,Q703,Q825,Q828,Q829</td>
    <td></td>
  </tr>
  <tr>
    <td>Renal and Genitourinary conditions</td>
    <td>N0,N11,N12,N13,N15,N18,N31,N32,<br>Q5,Q60,Q61,Q62,Q63,Q64</td>
    <td></td>
    <td></td>
    <td></td>
  </tr>
  <tr>
    <td>Prematurity and low birth weight</td>
    <td></td>
    <td>P05,P07</td>
    <td></td>
    <td></td>
  </tr>
  <tr>
    <td>Obesity</td>
    <td>E66</td>
    <td></td>
    <td></td>
    <td></td>
  </tr>
  <tr>
    <td>Pregnancy</td>
    <td>O</td>
    <td></td>
    <td></td>
    <td></td>
  </tr>
</tbody>
</table>
<br>
<br>

---

# Green Book Underlying Health Conditions (UHCs)

## Green Book UHC codelists

Machine-readable codelists are available within the `codelists/` folder as `.csv` files.  
 
* [`green_book_blood_disorders_and_immune_deficiencies.csv`](codelists/green_book_blood_disorders_and_immune_deficiencies.csv)
* [`green_book_cancer.csv`](codelists/green_book_cancer.csv)
* [`green_book_endocrine_conditions.csv`](codelists/green_book_endocrine_conditions.csv)
* [`green_book_severe_neurological_and_developmental_conditions.csv`](codelists/green_book_severe_neurological_and_developmental_conditions.csv)
* [`green_book_hypertension_and_cardiac_valves_and_cardiomyopathy.csv`](codelists/green_book_hypertension_and_cardiac_valves_and_cardiomyopathy.csv)
* [`green_book_severe_respiratory_diseases.csv`](codelists/green_book_severe_respiratory_diseases.csv)
* [`green_book_digestive_and_liver_diseases.csv`](codelists/green_book_digestive_and_liver_diseases.csv)
* [`green_book_renal_diseases.csv`](codelists/green_book_renal_diseases.csv)
* [`green_book_arthritis_and_connective_tissue_diseases.csv`](codelists/green_book_arthritis_and_connective_tissue_diseases.csv)
* [`green_book_congenital_syndromes_and_anomalies.csv`](codelists/green_book_congenital_syndromes_and_anomalies.csv)
* [`green_book_obesity.csv`](codelists/green_book_obesity.csv)
* [`green_book_pregnancy.csv`](codelists/green_book_pregnancy.csv)



💡 Users working within the NHS Digital Trusted Research Environment (TRE) should use the associated Databricks notebooks to implement these phenotypes in a more efficient manner!

**Schema**:
* `Phenotype` the same as the file-name  
* `Terminology` ICD-10 for all codes  
* `Code` the ICD-10 code  
  * ❗ Note some of the codes present are *partial* ICD-10 codes.
    * e.g. `E7` which includes codes spanning `E70` ('*Disorders of aromatic amino-acid metabolism*') to `E79.9` ('*Disorder of purine and pyrimidine metabolism, unspecified*')
    * This method was adopted to reduce the burden of specification on clinicians generating codelists.
    * These phenotypes are applied using partial string matching.
* `Description` is the description of the ICD-10 code
    * ❗ *partial* ICD-10 codes don't have an associated description. 
* `Filter` denotes whether the presence of the code determines inclusion or exclusion in the phenotype (`Include`/`Filter`, respectively)
  * Note the term `Filter` is used instead of Exclusion as the presence of a filter code does not exclude the patient having a phenotype, *if* they have another inclusion code
* `Lookback` denotes whether the previous time window over which a phenotype is valid
  * E.g. '9 months' for pregnancy


## Green Book UHC phenotyping algorithm

* Obesity (Note here BMI values and Weight-for-age / BMI-for-age Z-scores may imply   this UHC)
* Pregnancy (Note here the standard approach does not apply, we only count pregnancy  codes occurring in the previous 9 months OR within the admission if one is   present)

<table style="undefined;table-layout: fixed; width: 1734px">
<colgroup>
<col style="width: 247.005682px">
<col style="width: 517.005682px">
<col style="width: 134.005682px">
<col style="width: 702.005682px">
<col style="width: 134.005682px">
</colgroup>
<thead>
  <tr>
    <th>UHC Name</th>
    <th>Include Codes</th>
    <th>Include Codes <br>(5 Year Lookback)</th>
    <th>Include Code Chapter Filters</th>
    <th>Include Code <br>Chapter Filters   <br>(5 Year Lookback)</th>
  </tr>
</thead>
<tbody>
  <tr>
    <td>Cancer</td>
    <td></td>
    <td>C,D0,D4</td>
    <td></td>
    <td>D473</td>
  </tr>
  <tr>
    <td>Blood Disorders and Immune Deficiencies</td>
    <td>B20,B21,B22,B24,<br>D56,D57,D58,D61,D71,D730,D731,D8</td>
    <td></td>
    <td>D898,D899</td>
    <td></td>
  </tr>
  <tr>
    <td>Endocrine Conditions</td>
    <td>E10,E11,E12,E13,E14,E22,E23,E24,E25,E26,E27,E7,E80,E81,E82,E88,E89,E84</td>
    <td></td>
    <td>E231,E232,E739</td>
    <td></td>
  </tr>
  <tr>
    <td>Severe Neurological and Developmental Conditions</td>
    <td>F7,F80,F82,F83,F84,F85,F86,F87,F88,F89,<br>G1,G20,G21,G22,G23,G24,G26,G27,G28,G29,G3,G7,G8</td>
    <td></td>
    <td></td>
    <td></td>
  </tr>
  <tr>
    <td>Hypertension and Cardiac Valves and Cardiomyopathy</td>
    <td>I10,I11,I12,I13,I15,I27,I34,I35,I36,I37,I42,I43</td>
    <td></td>
    <td></td>
    <td></td>
  </tr>
  <tr>
    <td>Severe Respiratory Diseases</td>
    <td>J380,J386,J44,J45,J46,J47,J82,J84,J99</td>
    <td>P27</td>
    <td></td>
    <td></td>
  </tr>
  <tr>
    <td>Digestive and Liver Diseases</td>
    <td>K44,K50,K51,K52,K71,K721,K740,K741,K744,K745,K754,K75,K90</td>
    <td>K21,K22</td>
    <td>K210,K219,K220,K221,K222,K223,K224,K225,K226,K227,K228,K521,K522,K528,K529,K750,K751,K904,K908</td>
    <td></td>
  </tr>
  <tr>
    <td>Renal Diseases</td>
    <td>N0,N11,N12,N13,N15,N18</td>
    <td></td>
    <td></td>
    <td></td>
  </tr>
  <tr>
    <td>Arthritis and Connective Tissue Diseases</td>
    <td>M05,M06,M07,M08,M09,M3</td>
    <td></td>
    <td>M303,M357</td>
    <td></td>
  </tr>
  <tr>
    <td>Congenital Syndromes and Anomalies</td>
    <td>Q0,Q2,Q3,Q9,Q60,Q61,Q4,Q79,Q80,Q81,Q897</td>
    <td></td>
    <td>Q245,Q38</td>
    <td></td>
  </tr>
  <tr>
    <td>Obesity</td>
    <td>E66</td>
    <td></td>
    <td></td>
    <td></td>
  </tr>
  <tr>
    <td>Pregnancy</td>
    <td>O</td>
    <td></td>
    <td></td>
    <td></td>
  </tr>
</tbody>
</table>