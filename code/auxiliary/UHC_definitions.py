# Databricks notebook source
# Sourced from Kate's UHC definitions document

# COMMAND ----------

UHC = {
  "MD" : """name,includes,includes_5_years
Cancer and Neoplasms,,"C,D0,D37,D38,D39,D4"
Blood Disorders and Immune Deficiencies,"B20,B21,B22,B24,D55,D56,D57,D58,D59,D60,D61,D640,D644,D66,D67,D680,D681,D682,D70,D71,D720,D730,D731,D761,D763,D8,R16",
Endocrine Conditions,"E00,E10,E11,E12,E13,E14,E20,E21,E22,E23,E24,E25,E26,E27,E3,E4,E6,E7,E80,E83,E84,E85,E88,E89",
Neurological and Developmental Conditions,"F72,F73,F78,F79,F83,F84,F88,F89,G1,G20,G21,G22,G23,G24,G3,G40,G45,G47,G5,G6,G7,G8,G91,G94,P21,Q0,Q1,Q90,Q91,Q92,Q93,R568,R62,R94",
Respiratory Conditions,"J380,J386,J41,J42,J43,J44,J45,J46,J47,J82,J84,J984,J99,Q30,Q31,Q32,Q33,Q34",B27
Congenital Heart Disease and Hypertension and Acquired Heart Disease,"I05,I06,I07,I08,I09,I10,I11,I12,I13,I15,I27,I28,I31,I34,I35,I36,I37,I42,I43,I44,I45,I47,I48,I49,I50,I51,I52,Q2",
Digestive and Liver Conditions,"K44,K50,K51,K71,K721,K740,K741,K744,K745,K746,K75,K76,K90,Q35,Q36,Q37,Q39,Q4",
Muscle and Skin and Arthritis,"L1,L85,L93,L94,M05,M06,M07,M08,M09,M300,M301,M302,M31,M321,M328,M329,M34,M35,M41,M42,M43,Q65,Q67,Q71,Q72,Q73,Q74,Q75,Q76,Q77,Q78,Q79,Q80,Q81,Q85,Q87,Q89",
Renal and Genitourinary Conditions,"N0,N11,N12,N13,N15,N18,N31,N32,Q5,Q60,Q61,Q62,Q63,Q64",
Prematurity and Low Birth Weight,,"P05,P07"
Obesity,E66,
Pregnancy,O,""",
  "GREEN_BOOK" : """name,includes,includes_5_years
Cancer,,"C,D0"
Blood Disorders and Immune Deficiencies,"B20,B21,B22,B24,D56,D57,D58,D61,D71,D720,D730,D731,D8",
Endocrine Conditions,"E10,E11,E12,E13,E14,E22,E23,E24,E25,E26,E27,E70,E71,E72,E74,E75,E76,E79,E84",
Severe Neurological and Developmental Conditions,"F72,F73,F84,G1,G20,G21,G22,G23,G24,G26,G3,G40,G45,G7,G8",
Hypertension and Cardiac Valves and Cardiomyopathy,"I10,I11,I12,I13,I15,I27,I34,I35,I36,I37,I42,I43",
Severe Respiratory Diseases,"J380,J386,J44,J45,J46,J47,J82,J84,J99",B27
Digestive and Liver and Renal Diseases,"K44,K50,K51,K71,K721,K740,K741,K744,K745,K75,N03,N04,N183,N184,N185",
Arthritis and Connective Tissue Diseases,"M05,M06,M07,M08,M09,M300,M301,M302,M31,M321,M34,M35",
Congenital Syndromes and Anomalies,"Q0,Q20,Q210,Q212,Q213,Q214,Q218,Q22,Q23,Q242,Q244,Q245,Q246,Q25,Q26,Q31,Q32,Q33,Q34,Q39,Q442,Q60,Q61,Q79,Q80,Q81,Q897,Q9",
Obesity,E66,
Pregnancy,O,"""
}
