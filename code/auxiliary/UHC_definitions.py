# Databricks notebook source
# Sourced from Kate's UHC definitions document

# COMMAND ----------

UHC = {
    "MD" : """name,includes,includes_5_years,excludes,excludes_5_years
Blood Disorders and Immune Deficiencies,"B2,D55,D56,D57,D58,D60,D61,D64,D66,D67,D680,D681,D682,D71,D730,D731,D8",,"B25,B26,B27,B28,B29,D649",
Cancer and Neoplasms,,"C,D0,D4",,D473
Endocrine Conditions,"E0,E1,E2,E3,E4,E7,E8",,"E231,E232,E86,E87,E162",
Neurological and Developmental Conditions,"F7,F8,F9,G1,G2,G3,G4,G5,G6,G7,G8,G9,H17,H18,H19,H26,H33,H35,H40,H42,H46,H47,H48,H54,H90,H91,I673,I675,I69,P21,Q0,Q1,Q9,R62",,"F93,F94,F98,F99,G25,G43,G44,G45,G46,G92,G93,G96,G97,G98,H471",
Respiratory Conditions,"E84,J380,J386,J41,J42,J43,J44,J45,J46,J47,J82,J84,J99,Q30,Q31,Q32,Q33,Q34",P27,,
Congenital Heart Disease and Hypertension and Acquired Heart Disease,"I05,I06,I07,I08,I10,I11,I12,I13,I14,I15,I21,I22,I27,I28,I34,I35,I36,I37,I42,I43,I44,Q2",,Q245,
Digestive and Liver Conditions,"K44,K50,K51,K52,K71,K721,K740,K741,K744,K745,K75,K754,K90,Q35,Q36,Q37,Q39,Q4","K21,K22","K528,K529",
Muscle and Skin and Arthritis,"L1,L4,L85,L93,L94,M0,M1,M3,M41,M42,M43,Q65,Q66,Q67,Q68,Q69,Q7,Q8",,"M01,M02,M03,M04,M303,Q668,Q690,Q691,Q692,Q699,Q700,Q701,Q702,Q703,Q825,Q828,Q829",
Renal and Genitourinary Conditions,"N0,N11,N12,N13,N15,N18,N31,N32,Q5,Q60,Q61,Q62,Q63,Q64",,,
Prematurity and Low Birth Weight,,"P05,P07",,
Obesity,E66,,,
Pregnancy,O,,,""",
    "GREEN_BOOK" : """name,includes,includes_5_years,excludes,excludes_5_years
Cancer,,"C,D0,D4",,D473
Blood Disorders and Immune Deficiencies,"B20,B21,B22,B24,D56,D57,D58,D61,D71,D730,D731,D8",,"D898,D899",
Endocrine Conditions,"E10,E11,E12,E13,E14,E22,E23,E24,E25,E26,E27,E7,E80,E81,E82,E88,E89,E84",,"E231,E232,E739",
Severe Neurological and Developmental Conditions,"F7,F80,F82,F83,F84,F85,F86,F87,F88,F89,G1,G20,G21,G22,G23,G24,G26,G27,G28,G29,G3,G7,G8",,,
Hypertension and Cardiac Valves and Cardiomyopathy,"I10,I11,I12,I13,I15,I27,I34,I35,I36,I37,I42,I43",,,
Severe Respiratory Diseases,"J380,J386,J44,J45,J46,J47,J82,J84,J99",P27,,
Digestive and Liver Diseases,"K44,K50,K51,K52,K71,K721,K740,K741,K744,K745,K754,K75,K90","K21,K22","K210,K219,K220,K221,K222,K223,K224,K225,K226,K227,K228,K521,K522,K528,K529,K750,K751,K904,K908",
Renal Diseases,"N0,N11,N12,N13,N15,N18",,,
Arthritis and Connective Tissue Diseases,"M05,M06,M07,M08,M09,M3",,"M303,M357",
Congenital Syndromes and Anomalies,"Q0,Q2,Q3,Q9,Q60,Q61,Q4,Q79,Q80,Q81,Q897",,"Q245,Q38",
Obesity,E66,,,
Pregnancy,O,,,"""
}
