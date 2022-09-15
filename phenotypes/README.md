Phenotype code lists related to the analysis conducted for this project.

# Test HTML table

<style type="text/css">
.tg  {border-collapse:collapse;border-spacing:0;}
.tg td{border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;
  overflow:hidden;padding:10px 5px;word-break:normal;}
.tg th{border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;
  font-weight:normal;overflow:hidden;padding:10px 5px;word-break:normal;}
.tg .tg-sej6{border-color:inherit;font-family:"Lucida Console", Monaco, monospace !important;text-align:left;vertical-align:top}
.tg .tg-c3ow{border-color:inherit;text-align:center;vertical-align:top}
.tg .tg-0pky{border-color:inherit;text-align:left;vertical-align:top}
</style>
<table class="tg">
<thead>
  <tr>
    <th class="tg-c3ow"><span style="font-weight:bold">Population considered for inclusion</span></th>
    <th class="tg-c3ow"><span style="font-weight:bold">Typing hierarchy </span><br><span style="font-weight:bold">order</span></th>
    <th class="tg-c3ow"><span style="font-weight:bold">Group</span></th>
    <th class="tg-c3ow"><span style="font-weight:bold">Include Primary Codes</span></th>
    <th class="tg-c3ow"><span style="font-weight:bold">Exclude Primary Codes</span></th>
    <th class="tg-c3ow"><span style="font-weight:bold">Include Non-Primary Codes</span></th>
    <th class="tg-c3ow"><span style="font-weight:bold">Exclude Non-Primary Codes</span></th>
    <th class="tg-c3ow"><span style="font-weight:bold">Nosocomial (First instance of a SARS-CoV-2 related code / positive test on day 8 or later of admission)</span></th>
  </tr>
</thead>
<tbody>
  <tr>
    <td class="tg-0pky">All first hospital admissions occurring amongst first infections in our cohort as defined, <br>where the admission date is in the study era and there is one of:  <br>a) U071, U072 codes occurring in any position in their diagnosis (acute COVID-19 codes)  <br>b) U073, U074 codes occurring in a primary position only (the other two SARS-CoV-2 infection related codes)  <br>c) a PIMS-TS code (R65, M303, U075) alongside none of the exclude codes (see PIMS-TS row) after the date PIMS-TS was defined (May 2020)   <br>d) A positive test for COVID-19 14 days before hospitalisation up to the date of discharge.   <br>This description defines the <span style="font-weight:bold">study population with a SARS-CoV-2 related admission.</span></td>
    <td class="tg-0pky">1</td>
    <td class="tg-0pky"><span style="font-weight:bold">Nosocomial</span>: As defined by Healthcare-associated Covid-19 in England: A national data linkage study. Bhattacharya, Alex et al. Journal of Infection, Volume 83, Issue 5, 565 – 572</td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky">TRUE</td>
  </tr>
  <tr>
    <td class="tg-0pky">The above population minus Nosocomial admissions.</td>
    <td class="tg-0pky">2</td>
    <td class="tg-0pky"><span style="font-weight:bold">Type C</span>: Incidental cases where there is a primary diagnosis which appears unrelated to SARS-CoV-2 </td>
    <td class="tg-sej6">A00,A01,A02,A03,A04,A05,A06,A07,A080,A081,A082,A15,A16,A17,A18,A19,A2,A3,A5,A6,&nbsp;&nbsp;<br>B00,B01,B02,B03,B04,B05,B06,B07,B08,B1,B2,B3,B4,B5,B6,B7,B8,<br>D50,D51,D52,D53,D73,<br>E0,E2,E3,E5,<br>F,<br>G0,G5,G6,G9,<br>H,<br>I0,I60,I61,I62,I7,I8,<br>J6,J95,<br>K0,K1,K20,K25,K26,K27,K28,K29,K3,K4,K55,K56,K590,K58,K59,K6,K8,K9,<br>L0,L2,L3,L4,L5,L6,L72,L73,L8,L9,<br>M0,M1,M2,M4,M5,M6,M7,M8,M9,<br>N141,N2,N3,N39,N4,N5,N6,N7,N8,N9,<br>O,<br>P1,P20,P21,P50,P51,P52,P53,P54,P55,P56,P57,P58,P59,P6,P70,P71,P72,P75,P76,P77,P78,P8,P90,P91,P94,P95,P96,<br>Q1,Q4,Q5,Q6,Q7,Q8,<br>R12,R14,R15,R19,R20,R22,R23,R29,R30,R31,R32,R33,R35,R39,R45,R46,R80,R81,R82,R93,<br>U076,U82,U83,<br>S,<br>T,<br>V,<br>W,<br>X,<br>Y,<br>Z</td>
    <td class="tg-0pky">B20,B21,B22,B23,B24,F7,F8,H669,K44,Y4,Y5,Z038,Z039,U075,U109,M303,R65</td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky">U075,U109,M303,R65</td>
    <td class="tg-0pky"></td>
  </tr>
  <tr>
    <td class="tg-0pky">The above population minus Incidental admissions.</td>
    <td class="tg-0pky">3</td>
    <td class="tg-0pky"><span style="font-weight:bold">PIMS-TS</span>: All cases, with sepsis explicitly excluded due to code overlap</td>
    <td class="tg-0pky">U075,U109,M303,R65</td>
    <td class="tg-0pky">A01,A02,A03,A04,A05,A37,A38,A39,A40,A41,B95</td>
    <td class="tg-0pky">U075,U109,M303,R65</td>
    <td class="tg-0pky">A01,A02,A03,A04,A05,A37,A38,A39,A40,A41,B95</td>
    <td class="tg-0pky"></td>
  </tr>
  <tr>
    <td class="tg-0pky">The above population minus PIMS-TS admissions.</td>
    <td class="tg-0pky">4</td>
    <td class="tg-0pky"><span style="font-weight:bold">Type A1</span>: SARS-CoV-2 infection is listed as a primary reason for hospital admission</td>
    <td class="tg-0pky">U071,U072,U073,U074</td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
  </tr>
  <tr>
    <td class="tg-0pky">The above population minus admissions where a SARS-CoV-2 related code is in a primary position.</td>
    <td class="tg-0pky">5</td>
    <td class="tg-0pky"><span style="font-weight:bold">Type A2</span>: A Covid-19 symptom or typical/plausible presentations of Covid-19 is a primary reason for admission, alongside the presence of other evidence of SARS-CoV-2 infection via either a code or test</td>
    <td class="tg-0pky">A084,A083,A085,A090,A099,A419,B348,B309,B338,B349,B972,B99,D762,E86,E87,H669,I1254,I126,I1471,I254,I26,I288,I30,I31,I32,I33,I40,I41,I42,I44,I45,I46,I47,I48,I49,I50,I51,I63,I65,I66,I67,I880,I9,J00,J01,J04,J05,J06,J18,J22,J40,J80,J81,J83,J90,J93,J96,J98,K297,K529,M255,M791,M796,N179,P928,P25,P92,R0,R10,R11,R13,R17,R21,R25,R26,R27,R29,R34,R40,R41,R42,R43,R44,R47,R5,R6,R768,R845,R89,R90,R93,R94,U049,U070</td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
  </tr>
  <tr>
    <td class="tg-0pky">The above population minus Type A2 admissions.</td>
    <td class="tg-0pky">6</td>
    <td class="tg-0pky"><span style="font-weight:bold">Type B1</span>: Acute conditions linked to Covid-19 or that are known to co-occur with Covid-19</td>
    <td class="tg-0pky">A40,A41,A483,A49,A818,A870,A878,A89,A858,A86,A879,B09,B34,B95,B96,B97,E10,E11,E14,E16,G4,J02,J03,J09,J10,J11,J12,J13,J14,J15,J16,J17,J20,J21,J3,J85,J86,K859,P22,P23,P24,P26,P27,P28,P29,P3,P74,R70,R71,R72,R73,R74,R79,Z038,Z039,U075,U109,M303,R65</td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky">U075,U109,M303,R65</td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
  </tr>
  <tr>
    <td class="tg-0pky">The above population minus Type B1 admissions.</td>
    <td class="tg-0pky">7</td>
    <td class="tg-0pky"><span style="font-weight:bold">Type B2</span>: Underlying health conditions that are known to make patients sicker with Covid-19 or are conditions that make hospitalisation more likely</td>
    <td class="tg-0pky">B20,B21,B22,B24,D3,C,D0,D1,D2,D4,D55,D56,D57,D58,D59,D6,D70,D71,D72,D730,D731,D761,D76,D8,E10,E11,E12,E13,E14,E20,E21,E22,E23,E24,E25,E26,E27,E6,E7,E80,E81,E82,E83,E84,E850,E88,E89,F7,F8,G1,G2,G3,G7,G8,I10,I11,I12,I13,I15,I27,I34,I35,I36,I37,I42,I43,I675,J380,J386,J41,J42,J43,J44,J45,J46,J47,J82,J84,J99,K21,K22,K44,K50,K51,K52,K71,K720,K721,K740,K741,K744,K745,K746,K75,K76,K9,M3,N0,N10,N11,N12,N13,N15,N18,P0,Q0,Q2,Q3,Q60,Q61,Q9,R161</td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
  </tr>
  <tr>
    <td class="tg-0pky">The above population minus Type B2 admissions.</td>
    <td class="tg-0pky">8</td>
    <td class="tg-0pky"><span style="font-weight:bold">Exclude</span>: Any remaining admissions fall out of our types and thus may not be identifiably SARS-CoV-2 related; we exclude them all</td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
  </tr>
</tbody>
</table>