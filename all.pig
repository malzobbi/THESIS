REGISTER /usr/hadoop/trial.jar
/*
rmf hdfs://zobbi01:9000/input/G0
rmf hdfs://zobbi01:9000/input/G1
rmf hdfs://zobbi01:9000/input/NG0
rmf hdfs://zobbi01:9000/input/SG0
rmf hdfs://zobbi01:9000/input/NG1
rmf hdfs://zobbi01:9000/input/SG1
*/
--Load the Customers data from its location

Data = LOAD 'hdfs://zobbi01:9000/input/adult.csv' using PigStorage(',') as (f0:chararray,f1:chararray,f2:chararray,f3:chararray,f4:chararray,f5:chararray,f6:chararray,f7:chararray,f8:chararray,f9:chararray,f10:chararray);

--Filtered the data by the Salary

G0 = FILTER Data by (f3 matches ' 1st-4th');
G1 = FILTER Data by (f3 matches ' 5th-6th');
G2 = FILTER Data by (f3 matches ' 7th-8th');
G3 = FILTER Data by (f3 matches ' 9th');
G4 = FILTER Data by (f3 matches ' 10th');
G5 = FILTER Data by (f3 matches ' 11th');
G6 = FILTER Data by (f3 matches ' 12th');
G7 = FILTER Data by (f3 matches ' Assoc-voc');
G8 = FILTER Data by (f3 matches ' Some-college');
G9 = FILTER Data by (f3 matches ' Prof-school');
G10 = FILTER Data by (f3 matches ' Assoc-acdm');
G11 = FILTER Data by (f3 matches ' Bachelors');
G12 = FILTER Data by (f3 matches ' Preschool ');
G13 = FILTER Data by (f3 matches ' Masters');
G14 = FILTER Data by (f3 matches ' Doctorate');
G15 = FILTER Data by (f3 matches ' HS-grad');

--group each G

G00= group  G0 by (f0,f1,f2);
G000= foreach G00 generate COUNT (G0) as cnt0:long, G0.f0 as v0, G0.f1 as v1,G0.f2 as v2,G0.f3 as v3,G0.f4 as v4,G0.f5 as v5,G0.f6 as v6,G0.f7 as v7,G0.f8 as v8,G0.f9 as v9,G0.f10 as v10;
SG0= FILTER G000 by (cnt0 >= 20);
SG00 = FOREACH SG0  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE SG00 into 'hdfs://zobbi01:9000/output/1/SG0' using PigStorage(',');
SSG0= FILTER G000 by (cnt0 < 20);
SSG00 = FOREACH SSG0  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE SSG00 into 'hdfs://zobbi01:9000/input/1/SSG0' using PigStorage(',');


G101= group  G1 by (f0,f1,f2);
G1001= foreach G101 generate COUNT (G1) as cnt1:long, G1.f0 as v0, G1.f1 as v1,G1.f2 as v2,G1.f3 as v3,G1.f4 as v4,G1.f5 as v5,G1.f6 as v6,G1.f7 as v7,G1.f8 as v8,G1.f9 as v9,G1.f10 as v10;
SG1= FILTER G1001 by (cnt1 >= 20);
SG101 = FOREACH SG1  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE SG101 into 'hdfs://zobbi01:9000/output/1/SG1' using PigStorage(',');
SSG101= FILTER G1001 by (cnt1 < 20);
SSG101 = FOREACH SSG101  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE SSG101 into 'hdfs://zobbi01:9000/input/1/SSG1' using PigStorage(',');


G20= group  G2 by (f0,f1,f2);
G200= foreach G20 generate COUNT (G2) as cnt2:long, G2.f0 as v0, G2.f1 as v1,G2.f2 as v2,G2.f3 as v3,G2.f4 as v4,G2.f5 as v5,G2.f6 as v6,G2.f7 as v7,G2.f8 as v8,G2.f9 as v9,G2.f10 as v10;
SG2= FILTER G200 by (cnt2 >= 20);
SG20 = FOREACH SG2  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE SG20 into 'hdfs://zobbi01:9000/output/1/SG2' using PigStorage(',');
SSG2= FILTER G200 by (cnt2 < 20);
SSG20 = FOREACH SSG2  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE SSG20 into 'hdfs://zobbi01:9000/input/1/SSG2' using PigStorage(',');


G30= group  G3 by (f0,f1,f2);
G300= foreach G30 generate COUNT (G3) as cnt3:long, G3.f0 as v0, G3.f1 as v1,G3.f2 as v2,G3.f3 as v3,G3.f4 as v4,G3.f5 as v5,G3.f6 as v6,G3.f7 as v7,G3.f8 as v8,G3.f9 as v9,G3.f10 as v10;
SG3= FILTER G300 by (cnt3 >= 20);
SG30 = FOREACH SG3  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE SG30 into 'hdfs://zobbi01:9000/output/1/SG3' using PigStorage(',');
SSG3= FILTER G300 by (cnt3 < 20);
SSG30 = FOREACH SSG3  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE SSG30 into 'hdfs://zobbi01:9000/input/1/SSG3' using PigStorage(',');


G40= group  G4 by (f0,f1,f2);
G400= foreach G40 generate COUNT (G4) as cnt4:long, G4.f0 as v0, G4.f1 as v1,G4.f2 as v2,G4.f3 as v3,G4.f4 as v4,G4.f5 as v5,G4.f6 as v6,G4.f7 as v7,G4.f8 as v8,G4.f9 as v9,G4.f10 as v10;
SG4= FILTER G400 by (cnt4 >= 20);
SG40 = FOREACH SG4  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE SG40 into 'hdfs://zobbi01:9000/output/1/SG4' using PigStorage(',');
SSG4= FILTER G400 by (cnt4 < 20);
SSG40 = FOREACH SSG4  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE SSG40 into 'hdfs://zobbi01:9000/input/1/SSG4' using PigStorage(',');


G50= group  G5 by (f0,f1,f2);
G500= foreach G50 generate COUNT (G5) as cnt5:long, G5.f0 as v0, G5.f1 as v1,G5.f2 as v2,G5.f3 as v3,G5.f4 as v4,G5.f5 as v5,G5.f6 as v6,G5.f7 as v7,G5.f8 as v8,G5.f9 as v9,G5.f10 as v10;
SG5= FILTER G500 by (cnt5 >= 20);
SG50 = FOREACH SG5  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE SG50 into 'hdfs://zobbi01:9000/output/1/SG5' using PigStorage(',');
SSG5= FILTER G500 by (cnt5 < 20);
SSG50 = FOREACH SSG5  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE SSG50 into 'hdfs://zobbi01:9000/input/1/SSG5' using PigStorage(',');


G60= group  G6 by (f0,f1,f2);
G600= foreach G60 generate COUNT (G6) as cnt6:long, G6.f0 as v0, G6.f1 as v1,G6.f2 as v2,G6.f3 as v3,G6.f4 as v4,G6.f5 as v5,G6.f6 as v6,G6.f7 as v7,G6.f8 as v8,G6.f9 as v9,G6.f10 as v10;
SG6= FILTER G600 by (cnt6 >= 20);
SG60 = FOREACH SG6  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE SG60 into 'hdfs://zobbi01:9000/output/1/SG6' using PigStorage(',');
SSG6= FILTER G600 by (cnt6 < 20);
SSG60 = FOREACH SSG6  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE SSG60 into 'hdfs://zobbi01:9000/input/1/SSG6' using PigStorage(',');


G70= group  G7 by (f0,f1,f2);
G700= foreach G70 generate COUNT (G7) as cnt7:long, G7.f0 as v0, G7.f1 as v1,G7.f2 as v2,G7.f3 as v3,G7.f4 as v4,G7.f5 as v5,G7.f6 as v6,G7.f7 as v7,G7.f8 as v8,G7.f9 as v9,G7.f10 as v10;
SG7= FILTER G700 by (cnt7 >= 20);
SG70 = FOREACH SG7  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE SG70 into 'hdfs://zobbi01:9000/output/1/SG7' using PigStorage(',');
SSG7= FILTER G700 by (cnt7 < 20);
SSG70 = FOREACH SSG7  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE SSG70 into 'hdfs://zobbi01:9000/input/1/SSG7' using PigStorage(',');


G80= group  G8 by (f0,f1,f2);
G800= foreach G80 generate COUNT (G8) as cnt8:long, G8.f0 as v0, G8.f1 as v1,G8.f2 as v2,G8.f3 as v3,G8.f4 as v4,G8.f5 as v5,G8.f6 as v6,G8.f7 as v7,G8.f8 as v8,G8.f9 as v9,G8.f10 as v10;
SG8= FILTER G800 by (cnt8 >= 20);
SG80 = FOREACH SG8  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE SG80 into 'hdfs://zobbi01:9000/output/1/SG8' using PigStorage(',');
SSG8= FILTER G800 by (cnt8 < 20);
SSG80 = FOREACH SSG8  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE SSG80 into 'hdfs://zobbi01:9000/input/1/SSG8' using PigStorage(',');


G90= group  G9 by (f0,f1,f2);
G900= foreach G90 generate COUNT (G9) as cnt9:long, G9.f0 as v0, G9.f1 as v1,G9.f2 as v2,G9.f3 as v3,G9.f4 as v4,G9.f5 as v5,G9.f6 as v6,G9.f7 as v7,G9.f8 as v8,G9.f9 as v9,G9.f10 as v10;
SG9= FILTER G900 by (cnt9 >= 20);
SG90 = FOREACH SG9  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE SG90 into 'hdfs://zobbi01:9000/output/1/SG9' using PigStorage(',');
SSG9= FILTER G900 by (cnt9 < 20);
SSG90 = FOREACH SSG9  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE SSG90 into 'hdfs://zobbi01:9000/input/1/SSG9' using PigStorage(',');


G100= group  G10 by (f0,f1,f2);
G1000= foreach G100 generate COUNT (G10) as cnt10:long, G10.f0 as v0, G10.f1 as v1,G10.f2 as v2,G10.f3 as v3,G10.f4 as v4,G10.f5 as v5,G10.f6 as v6,G10.f7 as v7,G10.f8 as v8,G10.f9 as v9,G10.f10 as v10;
SG10= FILTER G1000 by (cnt10 >= 20);
SG100 = FOREACH SG10  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE SG100 into 'hdfs://zobbi01:9000/output/1/SG10' using PigStorage(',');
SSG10= FILTER G1000 by (cnt10 < 20);
SSG100 = FOREACH SSG10  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE SSG100 into 'hdfs://zobbi01:9000/input/1/SSG10' using PigStorage(',');
--DUMP SSG100;


G110= group  G11 by (f0,f1,f2);
G1100= foreach G110 generate COUNT (G11) as cnt11:long, G11.f0 as v0, G11.f1 as v1,G11.f2 as v2,G11.f3 as v3,G11.f4 as v4,G11.f5 as v5,G11.f6 as v6,G11.f7 as v7,G11.f8 as v8,G11.f9 as v9,G11.f10 as v10;
SG11= FILTER G1100 by (cnt11 >= 20);
SG110 = FOREACH SG11  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE SG110 into 'hdfs://zobbi01:9000/output/1/SG11' using PigStorage(',');
SSG11= FILTER G1100 by (cnt11 < 20);
SSG110 = FOREACH SSG11  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE SSG110 into 'hdfs://zobbi01:9000/input/1/SSG11' using PigStorage(',');


G120= group  G12 by (f0,f1,f2);
G1200= foreach G120 generate COUNT (G12) as cnt12:long, G12.f0 as v0, G12.f1 as v1,G12.f2 as v2,G12.f3 as v3,G12.f4 as v4,G12.f5 as v5,G12.f6 as v6,G12.f7 as v7,G12.f8 as v8,G12.f9 as v9,G12.f10 as v10;
SG12= FILTER G1200 by (cnt12 >= 20);
SG120 = FOREACH SG12  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE SG120 into 'hdfs://zobbi01:9000/output/1/SG12' using PigStorage(',');
SSG12= FILTER G1200 by (cnt12 < 20);
SSG120 = FOREACH SSG12  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE SSG120 into 'hdfs://zobbi01:9000/input/1/SSG12' using PigStorage(',');


G130= group  G13 by (f0,f1,f2);
G1300= foreach G130 generate COUNT (G13) as cnt13:long, G13.f0 as v0, G13.f1 as v1,G13.f2 as v2,G13.f3 as v3,G13.f4 as v4,G13.f5 as v5,G13.f6 as v6,G13.f7 as v7,G13.f8 as v8,G13.f9 as v9,G13.f10 as v10;
SG13= FILTER G1300 by (cnt13 >= 20);
SG130 = FOREACH SG13  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE SG130 into 'hdfs://zobbi01:9000/output/1/SG13' using PigStorage(',');
SSG13= FILTER G1300 by (cnt13 < 20);
SSG130 = FOREACH SSG13  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE SSG130 into 'hdfs://zobbi01:9000/input/1/SSG13' using PigStorage(',');


G140= group  G14 by (f0,f1,f2);
G1400= foreach G140 generate COUNT (G14) as cnt14:long, G14.f0 as v0, G14.f1 as v1,G14.f2 as v2,G14.f3 as v3,G14.f4 as v4,G14.f5 as v5,G14.f6 as v6,G14.f7 as v7,G14.f8 as v8,G14.f9 as v9,G14.f10 as v10;
SG14= FILTER G1400 by (cnt14 >= 20);
SG140 = FOREACH SG14  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE SG140 into 'hdfs://zobbi01:9000/output/1/SG14' using PigStorage(',');
SSG14= FILTER G1400 by (cnt14 < 20);
SSG140 = FOREACH SSG14  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE SSG140 into 'hdfs://zobbi01:9000/input/1/SSG14' using PigStorage(',');


G150= group  G15 by (f0,f1,f2);
G1500= foreach G150 generate COUNT (G15) as cnt15:long, G15.f0 as v0, G15.f1 as v1,G15.f2 as v2,G15.f3 as v3,G15.f4 as v4,G15.f5 as v5,G15.f6 as v6,G15.f7 as v7,G15.f8 as v8,G15.f9 as v9,G15.f10 as v10;
SG15= FILTER G1500 by (cnt15 >= 20);
SG150 = FOREACH SG15  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE SG150 into 'hdfs://zobbi01:9000/output/1/SG15' using PigStorage(',');
SSG15= FILTER G1500 by (cnt15 < 20);
SSG150 = FOREACH SSG15  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE SSG150 into 'hdfs://zobbi01:9000/input/1/SSG15' using PigStorage(',');

--GROUP with TWO Q-IDS
----/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
----///////////////////////////////////                                 \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
----/////////////////////////////////                                    \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

S_Data0 = LOAD 'hdfs://zobbi01:9000/input/1/SSG0' using PigStorage(',') as (f0:chararray,f1:chararray,f2:chararray,f3:chararray,f4:chararray,f5:chararray,f6:chararray,f7:chararray,f8:chararray,f9:chararray,f10:chararray);
S_G00= group  S_Data0 by (f1,f2);
S_G000= foreach S_G00 generate COUNT (S_Data0) as count0:long, S_Data0.f0 as v0, S_Data0.f1 as v1,S_Data0.f2 as v2,S_Data0.f3 as v3,S_Data0.f4 as v4,S_Data0.f5 as v5,S_Data0.f6 as v6,S_Data0.f7 as v7,S_Data0.f8 as v8,S_Data0.f9 as v9,S_Data0.f10 as v10;
S_SG0= FILTER S_G000 by (count0 >= 20);
S_SG00 = FOREACH S_SG0  GENERATE trial.SG(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SG00 into 'hdfs://zobbi01:9000/output/1/SG16' using PigStorage(',');
S_SSG0= FILTER S_G000 by (count0 < 20);
S_SSG00 = FOREACH S_SSG0  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SSG00 into 'hdfs://zobbi01:9000/input/1/SSG16' using PigStorage(',');


S_Data1 = LOAD 'hdfs://zobbi01:9000/input/1/SSG1' using PigStorage(',') as (f0:chararray,f1:chararray,f2:chararray,f3:chararray,f4:chararray,f5:chararray,f6:chararray,f7:chararray,f8:chararray,f9:chararray,f10:chararray);
S_G01= group  S_Data1 by (f1,f2);
S_G011= foreach S_G01 generate COUNT (S_Data1) as count1:long, S_Data1.f0 as v0, S_Data1.f1 as v1,S_Data1.f2 as v2,S_Data1.f3 as v3,S_Data1.f4 as v4,S_Data1.f5 as v5,S_Data1.f6 as v6,S_Data1.f7 as v7,S_Data1.f8 as v8,S_Data1.f9 as v9,S_Data1.f10 as v10;
S_SG1= FILTER S_G011 by (count1 >= 20);
S_SG01 = FOREACH S_SG1  GENERATE trial.SG(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SG01 into 'hdfs://zobbi01:9000/output/1/SG17' using PigStorage(',');
S_SSG1= FILTER S_G011 by (count1 < 20);
S_SSG01 = FOREACH S_SSG1  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SSG01 into 'hdfs://zobbi01:9000/input/1/SSG17' using PigStorage(',');


S_Data2 = LOAD 'hdfs://zobbi01:9000/input/1/SSG2' using PigStorage(',') as (f0:chararray,f1:chararray,f2:chararray,f3:chararray,f4:chararray,f5:chararray,f6:chararray,f7:chararray,f8:chararray,f9:chararray,f10:chararray);
S_G02= group  S_Data2 by (f1,f2);
S_G022= foreach S_G02 generate COUNT (S_Data2) as count2:long, S_Data2.f0 as v0, S_Data2.f1 as v1,S_Data2.f2 as v2,S_Data2.f3 as v3,S_Data2.f4 as v4,S_Data2.f5 as v5,S_Data2.f6 as v6,S_Data2.f7 as v7,S_Data2.f8 as v8,S_Data2.f9 as v9,S_Data2.f10 as v10;
S_SG2= FILTER S_G022 by (count2 >= 20);
S_SG02 = FOREACH S_SG2  GENERATE trial.SG(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SG02 into 'hdfs://zobbi01:9000/output/1/SG18' using PigStorage(',');
S_SSG2= FILTER S_G022 by (count2 < 20);
S_SSG02 = FOREACH S_SSG2  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SSG02 into 'hdfs://zobbi01:9000/input/1/SSG18' using PigStorage(',');


S_Data3 = LOAD 'hdfs://zobbi01:9000/input/1/SSG3' using PigStorage(',') as (f0:chararray,f1:chararray,f2:chararray,f3:chararray,f4:chararray,f5:chararray,f6:chararray,f7:chararray,f8:chararray,f9:chararray,f10:chararray);
S_G03= group  S_Data3 by (f1,f2);
S_G033= foreach S_G03 generate COUNT (S_Data3) as count3:long, S_Data3.f0 as v0, S_Data3.f1 as v1,S_Data3.f2 as v2,S_Data3.f3 as v3,S_Data3.f4 as v4,S_Data3.f5 as v5,S_Data3.f6 as v6,S_Data3.f7 as v7,S_Data3.f8 as v8,S_Data3.f9 as v9,S_Data3.f10 as v10;
S_SG3= FILTER S_G033 by (count3 >= 20);
S_SG03 = FOREACH S_SG3  GENERATE trial.SG(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SG03 into 'hdfs://zobbi01:9000/output/1/SG19' using PigStorage(',');
S_SSG3= FILTER S_G033 by (count3 < 20);
S_SSG03 = FOREACH S_SSG3  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SSG03 into 'hdfs://zobbi01:9000/input/1/SSG19' using PigStorage(',');


S_Data4 = LOAD 'hdfs://zobbi01:9000/input/1/SSG4' using PigStorage(',') as (f0:chararray,f1:chararray,f2:chararray,f3:chararray,f4:chararray,f5:chararray,f6:chararray,f7:chararray,f8:chararray,f9:chararray,f10:chararray);
S_G04= group  S_Data4 by (f1,f2);
S_G044= foreach S_G04 generate COUNT (S_Data4) as count4:long, S_Data4.f0 as v0, S_Data4.f1 as v1,S_Data4.f2 as v2,S_Data4.f3 as v3,S_Data4.f4 as v4,S_Data4.f5 as v5,S_Data4.f6 as v6,S_Data4.f7 as v7,S_Data4.f8 as v8,S_Data4.f9 as v9,S_Data4.f10 as v10;
S_SG4= FILTER S_G044 by (count4 >= 20);
S_SG04 = FOREACH S_SG4  GENERATE trial.SG(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SG04 into 'hdfs://zobbi01:9000/output/1/SG20' using PigStorage(',');
S_SSG4= FILTER S_G044 by (count4 < 20);
S_SSG04 = FOREACH S_SSG4  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SSG04 into 'hdfs://zobbi01:9000/input/1/SSG20' using PigStorage(',');


S_Data5 = LOAD 'hdfs://zobbi01:9000/input/1/SSG5' using PigStorage(',') as (f0:chararray,f1:chararray,f2:chararray,f3:chararray,f4:chararray,f5:chararray,f6:chararray,f7:chararray,f8:chararray,f9:chararray,f10:chararray);
S_G05= group  S_Data5 by (f1,f2);
S_G055= foreach S_G05 generate COUNT (S_Data5) as count5:long, S_Data5.f0 as v0, S_Data5.f1 as v1,S_Data5.f2 as v2,S_Data5.f3 as v3,S_Data5.f4 as v4,S_Data5.f5 as v5,S_Data5.f6 as v6,S_Data5.f7 as v7,S_Data5.f8 as v8,S_Data5.f9 as v9,S_Data5.f10 as v10;
S_SG5= FILTER S_G055 by (count5 >= 20);
S_SG05 = FOREACH S_SG5  GENERATE trial.SG(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SG05 into 'hdfs://zobbi01:9000/output/1/SG21' using PigStorage(',');
S_SSG5= FILTER S_G055 by (count5 < 20);
S_SSG05 = FOREACH S_SSG5  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SSG05 into 'hdfs://zobbi01:9000/input/1/SSG21' using PigStorage(',');


S_Data6 = LOAD 'hdfs://zobbi01:9000/input/1/SSG6' using PigStorage(',') as (f0:chararray,f1:chararray,f2:chararray,f3:chararray,f4:chararray,f5:chararray,f6:chararray,f7:chararray,f8:chararray,f9:chararray,f10:chararray);
S_G06= group  S_Data6 by (f1,f2);
S_G066= foreach S_G06 generate COUNT (S_Data6) as count6:long, S_Data6.f0 as v0, S_Data6.f1 as v1,S_Data6.f2 as v2,S_Data6.f3 as v3,S_Data6.f4 as v4,S_Data6.f5 as v5,S_Data6.f6 as v6,S_Data6.f7 as v7,S_Data6.f8 as v8,S_Data6.f9 as v9,S_Data6.f10 as v10;
S_SG6= FILTER S_G066 by (count6 >= 20);
S_SG06 = FOREACH S_SG6  GENERATE trial.SG(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SG06 into 'hdfs://zobbi01:9000/output/1/SG22' using PigStorage(',');
S_SSG6= FILTER S_G066 by (count6 < 20);
S_SSG06 = FOREACH S_SSG6  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SSG06 into 'hdfs://zobbi01:9000/input/1/SSG22' using PigStorage(',');


S_Data7 = LOAD 'hdfs://zobbi01:9000/input/1/SSG7' using PigStorage(',') as (f0:chararray,f1:chararray,f2:chararray,f3:chararray,f4:chararray,f5:chararray,f6:chararray,f7:chararray,f8:chararray,f9:chararray,f10:chararray);
S_G07= group  S_Data7 by (f1,f2);
S_G077= foreach S_G07 generate COUNT (S_Data7) as count7:long, S_Data7.f0 as v0, S_Data7.f1 as v1,S_Data7.f2 as v2,S_Data7.f3 as v3,S_Data7.f4 as v4,S_Data7.f5 as v5,S_Data7.f6 as v6,S_Data7.f7 as v7,S_Data7.f8 as v8,S_Data7.f9 as v9,S_Data7.f10 as v10;
S_SG7= FILTER S_G077 by (count7 >= 20);
S_SG07 = FOREACH S_SG7  GENERATE trial.SG(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SG07 into 'hdfs://zobbi01:9000/output/1/SG23' using PigStorage(',');
S_SSG7= FILTER S_G077 by (count7 < 20);
S_SSG07 = FOREACH S_SSG7  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SSG07 into 'hdfs://zobbi01:9000/input/1/SSG23' using PigStorage(',');


S_Data8 = LOAD 'hdfs://zobbi01:9000/input/1/SSG8' using PigStorage(',') as (f0:chararray,f1:chararray,f2:chararray,f3:chararray,f4:chararray,f5:chararray,f6:chararray,f7:chararray,f8:chararray,f9:chararray,f10:chararray);
S_G08= group  S_Data8 by (f1,f2);
S_G088= foreach S_G08 generate COUNT (S_Data8) as count8:long, S_Data8.f0 as v0, S_Data8.f1 as v1,S_Data8.f2 as v2,S_Data8.f3 as v3,S_Data8.f4 as v4,S_Data8.f5 as v5,S_Data8.f6 as v6,S_Data8.f7 as v7,S_Data8.f8 as v8,S_Data8.f9 as v9,S_Data8.f10 as v10;
S_SG8= FILTER S_G088 by (count8 >= 20);
S_SG08 = FOREACH S_SG8  GENERATE trial.SG(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SG08 into 'hdfs://zobbi01:9000/output/1/SG24' using PigStorage(',');
S_SSG8= FILTER S_G088 by (count8 < 20);
S_SSG08 = FOREACH S_SSG8  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SSG08 into 'hdfs://zobbi01:9000/input/1/SSG24' using PigStorage(',');


S_Data9 = LOAD 'hdfs://zobbi01:9000/input/1/SSG9' using PigStorage(',') as (f0:chararray,f1:chararray,f2:chararray,f3:chararray,f4:chararray,f5:chararray,f6:chararray,f7:chararray,f8:chararray,f9:chararray,f10:chararray);
S_G09= group  S_Data9 by (f1,f2);
S_G099= foreach S_G09 generate COUNT (S_Data9) as count9:long, S_Data9.f0 as v0, S_Data9.f1 as v1,S_Data9.f2 as v2,S_Data9.f3 as v3,S_Data9.f4 as v4,S_Data9.f5 as v5,S_Data9.f6 as v6,S_Data9.f7 as v7,S_Data9.f8 as v8,S_Data9.f9 as v9,S_Data9.f10 as v10;
S_SG9= FILTER S_G099 by (count9 >= 20);
S_SG09 = FOREACH S_SG9  GENERATE trial.SG(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SG09 into 'hdfs://zobbi01:9000/output/1/SG25' using PigStorage(',');
S_SSG9= FILTER S_G099 by (count9 < 20);
S_SSG09 = FOREACH S_SSG9  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SSG09 into 'hdfs://zobbi01:9000/input/1/SSG25' using PigStorage(',');


S_Data10 = LOAD 'hdfs://zobbi01:9000/input/1/SSG10' using PigStorage(',') as (f0:chararray,f1:chararray,f2:chararray,f3:chararray,f4:chararray,f5:chararray,f6:chararray,f7:chararray,f8:chararray,f9:chararray,f10:chararray);
S_G010= group  S_Data10 by (f1,f2);
S_G01010= foreach S_G010 generate COUNT (S_Data10) as count10:long, S_Data10.f0 as v0, S_Data10.f1 as v1,S_Data10.f2 as v2,S_Data10.f3 as v3,S_Data10.f4 as v4,S_Data10.f5 as v5,S_Data10.f6 as v6,S_Data10.f7 as v7,S_Data10.f8 as v8,S_Data10.f9 as v9,S_Data10.f10 as v10;
S_SG10= FILTER S_G01010 by (count10 >= 20);
S_SG010 = FOREACH S_SG10  GENERATE trial.SG(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SG010 into 'hdfs://zobbi01:9000/output/1/SG26' using PigStorage(',');
S_SSG10= FILTER S_G01010 by (count10 < 20);
S_SSG010 = FOREACH S_SSG10  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SSG010 into 'hdfs://zobbi01:9000/input/1/SSG26' using PigStorage(',');


S_Data11 = LOAD 'hdfs://zobbi01:9000/input/1/SSG11' using PigStorage(',') as (f0:chararray,f1:chararray,f2:chararray,f3:chararray,f4:chararray,f5:chararray,f6:chararray,f7:chararray,f8:chararray,f9:chararray,f10:chararray);
S_G011= group  S_Data11 by (f1,f2);
S_G01111= foreach S_G011 generate COUNT (S_Data11) as count11:long, S_Data11.f0 as v0, S_Data11.f1 as v1,S_Data11.f2 as v2,S_Data11.f3 as v3,S_Data11.f4 as v4,S_Data11.f5 as v5,S_Data11.f6 as v6,S_Data11.f7 as v7,S_Data11.f8 as v8,S_Data11.f9 as v9,S_Data11.f10 as v10;
S_SG11= FILTER S_G01111 by (count11 >= 20);
S_SG011 = FOREACH S_SG11  GENERATE trial.SG(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SG011 into 'hdfs://zobbi01:9000/output/1/SG27' using PigStorage(',');
S_SSG11= FILTER S_G01111 by (count11 < 20);
S_SSG011 = FOREACH S_SSG11  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SSG011 into 'hdfs://zobbi01:9000/input/1/SSG27' using PigStorage(',');


S_Data12 = LOAD 'hdfs://zobbi01:9000/input/1/SSG12' using PigStorage(',') as (f0:chararray,f1:chararray,f2:chararray,f3:chararray,f4:chararray,f5:chararray,f6:chararray,f7:chararray,f8:chararray,f9:chararray,f10:chararray);
S_G012= group  S_Data12 by (f1,f2);
S_G01212= foreach S_G012 generate COUNT (S_Data12) as count12:long, S_Data12.f0 as v0, S_Data12.f1 as v1,S_Data12.f2 as v2,S_Data12.f3 as v3,S_Data12.f4 as v4,S_Data12.f5 as v5,S_Data12.f6 as v6,S_Data12.f7 as v7,S_Data12.f8 as v8,S_Data12.f9 as v9,S_Data12.f10 as v10;
S_SG12= FILTER S_G01212 by (count12 >= 20);
S_SG012 = FOREACH S_SG12  GENERATE trial.SG(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SG012 into 'hdfs://zobbi01:9000/output/1/SG28' using PigStorage(',');
S_SSG12= FILTER S_G01212 by (count12 < 20);
S_SSG012 = FOREACH S_SSG12  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SSG012 into 'hdfs://zobbi01:9000/input/1/SSG28' using PigStorage(',');


S_Data13 = LOAD 'hdfs://zobbi01:9000/input/1/SSG13' using PigStorage(',') as (f0:chararray,f1:chararray,f2:chararray,f3:chararray,f4:chararray,f5:chararray,f6:chararray,f7:chararray,f8:chararray,f9:chararray,f10:chararray);
S_G013= group  S_Data13 by (f1,f2);
S_G01313= foreach S_G013 generate COUNT (S_Data13) as count13:long, S_Data13.f0 as v0, S_Data13.f1 as v1,S_Data13.f2 as v2,S_Data13.f3 as v3,S_Data13.f4 as v4,S_Data13.f5 as v5,S_Data13.f6 as v6,S_Data13.f7 as v7,S_Data13.f8 as v8,S_Data13.f9 as v9,S_Data13.f10 as v10;
S_SG13= FILTER S_G01313 by (count13 >= 20);
S_SG013 = FOREACH S_SG13  GENERATE trial.SG(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SG013 into 'hdfs://zobbi01:9000/output/1/SG29' using PigStorage(',');
S_SSG13= FILTER S_G01313 by (count13 < 20);
S_SSG013 = FOREACH S_SSG13  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SSG013 into 'hdfs://zobbi01:9000/input/1/SSG29' using PigStorage(',');


S_Data14 = LOAD 'hdfs://zobbi01:9000/input/1/SSG14' using PigStorage(',') as (f0:chararray,f1:chararray,f2:chararray,f3:chararray,f4:chararray,f5:chararray,f6:chararray,f7:chararray,f8:chararray,f9:chararray,f10:chararray);
S_G014= group  S_Data14 by (f1,f2);
S_G01414= foreach S_G014 generate COUNT (S_Data14) as count14:long, S_Data14.f0 as v0, S_Data14.f1 as v1,S_Data14.f2 as v2,S_Data14.f3 as v3,S_Data14.f4 as v4,S_Data14.f5 as v5,S_Data14.f6 as v6,S_Data14.f7 as v7,S_Data14.f8 as v8,S_Data14.f9 as v9,S_Data14.f10 as v10;
S_SG14= FILTER S_G01414 by (count14 >= 20);
S_SG014 = FOREACH S_SG14  GENERATE trial.SG(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SG014 into 'hdfs://zobbi01:9000/output/1/SG30' using PigStorage(',');
S_SSG14= FILTER S_G01414 by (count14 < 20);
S_SSG014 = FOREACH S_SSG14  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SSG014 into 'hdfs://zobbi01:9000/input/1/SSG30' using PigStorage(',');


S_Data15 = LOAD 'hdfs://zobbi01:9000/input/1/SSG15' using PigStorage(',') as (f0:chararray,f1:chararray,f2:chararray,f3:chararray,f4:chararray,f5:chararray,f6:chararray,f7:chararray,f8:chararray,f9:chararray,f10:chararray);
S_G015= group  S_Data15 by (f1,f2);
S_G01515= foreach S_G015 generate COUNT (S_Data15) as count15:long, S_Data15.f0 as v0, S_Data15.f1 as v1,S_Data15.f2 as v2,S_Data15.f3 as v3,S_Data15.f4 as v4,S_Data15.f5 as v5,S_Data15.f6 as v6,S_Data15.f7 as v7,S_Data15.f8 as v8,S_Data15.f9 as v9,S_Data15.f10 as v10;
S_SG15= FILTER S_G01515 by (count15 >= 20);
S_SG015 = FOREACH S_SG15  GENERATE trial.SG(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SG015 into 'hdfs://zobbi01:9000/output/1/SG31' using PigStorage(',');
S_SSG15= FILTER S_G01515 by (count15 < 20);
S_SSG015 = FOREACH S_SSG15  GENERATE trial.adjust(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SSG015 into 'hdfs://zobbi01:9000/input/1/SSG31' using PigStorage(',');

--GROUP with ONE Q-IDS
----/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
----///////////////////////////////////                                 \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
----/////////////////////////////////                                    \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

S_Data16 = Load 'hdfs://zobbi01:9000/input/1/SSG16' using PigStorage(',') as (f0:chararray,f1:chararray,f2:chararray,f3:chararray,f4:chararray,f5:chararray,f6:chararray,f7:chararray,f8:chararray,f9:chararray,f10:chararray);
S_G016= group  S_Data16 by (f2);
S_G01616= foreach S_G016 generate COUNT (S_Data16) as count16:long, S_Data16.f0 as v0, S_Data16.f1 as v1,S_Data16.f2 as v2,S_Data16.f3 as v3,S_Data16.f4 as v4,S_Data16.f5 as v5,S_Data16.f6 as v6,S_Data16.f7 as v7,S_Data16.f8 as v8,S_Data16.f9 as v9,S_Data16.f10 as v10;
S_SG16= FILTER S_G01616 by (count16 >= 20);
S_SG016 = FOREACH S_SG16  GENERATE trial.NG_interval(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SG016 into 'hdfs://zobbi01:9000/output/1/SG32' using PigStorage(',');
S_SSG16= FILTER S_G01616 by (count16 < 20);
S_SSG016 = FOREACH S_SSG16  GENERATE trial.suppress(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SSG016 into 'hdfs://zobbi01:9000/output/1/SG_SP32' using PigStorage(',');


S_Data17 = Load 'hdfs://zobbi01:9000/input/1/SSG17' using PigStorage(',') as (f0:chararray,f1:chararray,f2:chararray,f3:chararray,f4:chararray,f5:chararray,f6:chararray,f7:chararray,f8:chararray,f9:chararray,f10:chararray);
S_G017= group  S_Data17 by (f2);
S_G01717= foreach S_G017 generate COUNT (S_Data17) as count17:long, S_Data17.f0 as v0, S_Data17.f1 as v1,S_Data17.f2 as v2,S_Data17.f3 as v3,S_Data17.f4 as v4,S_Data17.f5 as v5,S_Data17.f6 as v6,S_Data17.f7 as v7,S_Data17.f8 as v8,S_Data17.f9 as v9,S_Data17.f10 as v10;
S_SG17= FILTER S_G01717 by (count17 >= 20);
S_SG017 = FOREACH S_SG17  GENERATE trial.NG_interval(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SG017 into 'hdfs://zobbi01:9000/output/1/SG33' using PigStorage(',');
S_SSG17= FILTER S_G01717 by (count17 < 20);
S_SSG017 = FOREACH S_SSG17  GENERATE trial.suppress(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SSG017 into 'hdfs://zobbi01:9000/output/1/SG_SP33' using PigStorage(',');


S_Data18 = Load 'hdfs://zobbi01:9000/input/1/SSG18' using PigStorage(',') as (f0:chararray,f1:chararray,f2:chararray,f3:chararray,f4:chararray,f5:chararray,f6:chararray,f7:chararray,f8:chararray,f9:chararray,f10:chararray);
S_G018= group  S_Data18 by (f2);
S_G01818= foreach S_G018 generate COUNT (S_Data18) as count18:long, S_Data18.f0 as v0, S_Data18.f1 as v1,S_Data18.f2 as v2,S_Data18.f3 as v3,S_Data18.f4 as v4,S_Data18.f5 as v5,S_Data18.f6 as v6,S_Data18.f7 as v7,S_Data18.f8 as v8,S_Data18.f9 as v9,S_Data18.f10 as v10;
S_SG18= FILTER S_G01818 by (count18 >= 20);
S_SG018 = FOREACH S_SG18  GENERATE trial.NG_interval(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SG018 into 'hdfs://zobbi01:9000/output/1/SG34' using PigStorage(',');
S_SSG18= FILTER S_G01818 by (count18 < 20);
S_SSG018 = FOREACH S_SSG18  GENERATE trial.suppress(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SSG018 into 'hdfs://zobbi01:9000/output/1/SG_SP34' using PigStorage(',');


S_Data19 = Load 'hdfs://zobbi01:9000/input/1/SSG19' using PigStorage(',') as (f0:chararray,f1:chararray,f2:chararray,f3:chararray,f4:chararray,f5:chararray,f6:chararray,f7:chararray,f8:chararray,f9:chararray,f10:chararray);
S_G019= group  S_Data19 by (f2);
S_G01919= foreach S_G019 generate COUNT (S_Data19) as count19:long, S_Data19.f0 as v0, S_Data19.f1 as v1,S_Data19.f2 as v2,S_Data19.f3 as v3,S_Data19.f4 as v4,S_Data19.f5 as v5,S_Data19.f6 as v6,S_Data19.f7 as v7,S_Data19.f8 as v8,S_Data19.f9 as v9,S_Data19.f10 as v10;
S_SG19= FILTER S_G01919 by (count19 >= 20);
S_SG019 = FOREACH S_SG19  GENERATE trial.NG_interval(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SG019 into 'hdfs://zobbi01:9000/output/1/SG35' using PigStorage(',');
S_SSG19= FILTER S_G01919 by (count19 < 20);
S_SSG019 = FOREACH S_SSG19  GENERATE trial.suppress(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SSG019 into 'hdfs://zobbi01:9000/output/1/SG_SP35' using PigStorage(',');


S_Data20 = Load 'hdfs://zobbi01:9000/input/1/SSG20' using PigStorage(',') as (f0:chararray,f1:chararray,f2:chararray,f3:chararray,f4:chararray,f5:chararray,f6:chararray,f7:chararray,f8:chararray,f9:chararray,f10:chararray);
S_G020= group  S_Data20 by (f2);
S_G02020= foreach S_G020 generate COUNT (S_Data20) as count20:long, S_Data20.f0 as v0, S_Data20.f1 as v1,S_Data20.f2 as v2,S_Data20.f3 as v3,S_Data20.f4 as v4,S_Data20.f5 as v5,S_Data20.f6 as v6,S_Data20.f7 as v7,S_Data20.f8 as v8,S_Data20.f9 as v9,S_Data20.f10 as v10;
S_SG20= FILTER S_G02020 by (count20 >= 20);
S_SG020 = FOREACH S_SG20  GENERATE trial.NG_interval(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SG020 into 'hdfs://zobbi01:9000/output/1/SG36' using PigStorage(',');
S_SSG20= FILTER S_G02020 by (count20 < 20);
S_SSG020 = FOREACH S_SSG20  GENERATE trial.suppress(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SSG020 into 'hdfs://zobbi01:9000/output/1/SG_SP36' using PigStorage(',');


S_Data21 = Load 'hdfs://zobbi01:9000/input/1/SSG21' using PigStorage(',') as (f0:chararray,f1:chararray,f2:chararray,f3:chararray,f4:chararray,f5:chararray,f6:chararray,f7:chararray,f8:chararray,f9:chararray,f10:chararray);
S_G021= group  S_Data21 by (f2);
S_G02121= foreach S_G021 generate COUNT (S_Data21) as count21:long, S_Data21.f0 as v0, S_Data21.f1 as v1,S_Data21.f2 as v2,S_Data21.f3 as v3,S_Data21.f4 as v4,S_Data21.f5 as v5,S_Data21.f6 as v6,S_Data21.f7 as v7,S_Data21.f8 as v8,S_Data21.f9 as v9,S_Data21.f10 as v10;
S_SG21= FILTER S_G02121 by (count21 >= 20);
S_SG021 = FOREACH S_SG21  GENERATE trial.NG_interval(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SG021 into 'hdfs://zobbi01:9000/output/1/SG37' using PigStorage(',');
S_SSG21= FILTER S_G02121 by (count21 < 20);
S_SSG021 = FOREACH S_SSG21  GENERATE trial.suppress(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SSG021 into 'hdfs://zobbi01:9000/output/1/SG_SP37' using PigStorage(',');


S_Data22 = Load 'hdfs://zobbi01:9000/input/1/SSG22' using PigStorage(',') as (f0:chararray,f1:chararray,f2:chararray,f3:chararray,f4:chararray,f5:chararray,f6:chararray,f7:chararray,f8:chararray,f9:chararray,f10:chararray);
S_G022= group  S_Data22 by (f2);
S_G02222= foreach S_G022 generate COUNT (S_Data22) as count22:long, S_Data22.f0 as v0, S_Data22.f1 as v1,S_Data22.f2 as v2,S_Data22.f3 as v3,S_Data22.f4 as v4,S_Data22.f5 as v5,S_Data22.f6 as v6,S_Data22.f7 as v7,S_Data22.f8 as v8,S_Data22.f9 as v9,S_Data22.f10 as v10;
S_SG22= FILTER S_G02222 by (count22 >= 20);
S_SG022 = FOREACH S_SG22  GENERATE trial.NG_interval(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SG022 into 'hdfs://zobbi01:9000/output/1/SG38' using PigStorage(',');
S_SSG22= FILTER S_G02222 by (count22 < 20);
S_SSG022 = FOREACH S_SSG22  GENERATE trial.suppress(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SSG022 into 'hdfs://zobbi01:9000/output/1/SG_SP38' using PigStorage(',');


S_Data23 = Load 'hdfs://zobbi01:9000/input/1/SSG23' using PigStorage(',') as (f0:chararray,f1:chararray,f2:chararray,f3:chararray,f4:chararray,f5:chararray,f6:chararray,f7:chararray,f8:chararray,f9:chararray,f10:chararray);
S_G023= group  S_Data23 by (f2);
S_G02323= foreach S_G023 generate COUNT (S_Data23) as count23:long, S_Data23.f0 as v0, S_Data23.f1 as v1,S_Data23.f2 as v2,S_Data23.f3 as v3,S_Data23.f4 as v4,S_Data23.f5 as v5,S_Data23.f6 as v6,S_Data23.f7 as v7,S_Data23.f8 as v8,S_Data23.f9 as v9,S_Data23.f10 as v10;
S_SG23= FILTER S_G02323 by (count23 >= 20);
S_SG023 = FOREACH S_SG23  GENERATE trial.NG_interval(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SG023 into 'hdfs://zobbi01:9000/output/1/SG39' using PigStorage(',');
S_SSG23= FILTER S_G02323 by (count23 < 20);
S_SSG023 = FOREACH S_SSG23  GENERATE trial.suppress(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SSG023 into 'hdfs://zobbi01:9000/output/1/SG_SP39' using PigStorage(',');


S_Data24 = Load 'hdfs://zobbi01:9000/input/1/SSG24' using PigStorage(',') as (f0:chararray,f1:chararray,f2:chararray,f3:chararray,f4:chararray,f5:chararray,f6:chararray,f7:chararray,f8:chararray,f9:chararray,f10:chararray);
S_G024= group  S_Data24 by (f2);
S_G02424= foreach S_G024 generate COUNT (S_Data24) as count24:long, S_Data24.f0 as v0, S_Data24.f1 as v1,S_Data24.f2 as v2,S_Data24.f3 as v3,S_Data24.f4 as v4,S_Data24.f5 as v5,S_Data24.f6 as v6,S_Data24.f7 as v7,S_Data24.f8 as v8,S_Data24.f9 as v9,S_Data24.f10 as v10;
S_SG24= FILTER S_G02424 by (count24 >= 20);
S_SG024 = FOREACH S_SG24  GENERATE trial.NG_interval(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SG024 into 'hdfs://zobbi01:9000/output/1/SG40' using PigStorage(',');
S_SSG24= FILTER S_G02424 by (count24 < 20);
S_SSG024 = FOREACH S_SSG24  GENERATE trial.suppress(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SSG024 into 'hdfs://zobbi01:9000/output/1/SG_SP40' using PigStorage(',');


S_Data25 = Load 'hdfs://zobbi01:9000/input/1/SSG25' using PigStorage(',') as (f0:chararray,f1:chararray,f2:chararray,f3:chararray,f4:chararray,f5:chararray,f6:chararray,f7:chararray,f8:chararray,f9:chararray,f10:chararray);
S_G025= group  S_Data25 by (f2);
S_G02525= foreach S_G025 generate COUNT (S_Data25) as count25:long, S_Data25.f0 as v0, S_Data25.f1 as v1,S_Data25.f2 as v2,S_Data25.f3 as v3,S_Data25.f4 as v4,S_Data25.f5 as v5,S_Data25.f6 as v6,S_Data25.f7 as v7,S_Data25.f8 as v8,S_Data25.f9 as v9,S_Data25.f10 as v10;
S_SG25= FILTER S_G02525 by (count25 >= 20);
S_SG025 = FOREACH S_SG25  GENERATE trial.NG_interval(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SG025 into 'hdfs://zobbi01:9000/output/1/SG41' using PigStorage(',');
S_SSG25= FILTER S_G02525 by (count25 < 20);
S_SSG025 = FOREACH S_SSG25  GENERATE trial.suppress(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SSG025 into 'hdfs://zobbi01:9000/output/1/SG_SP41' using PigStorage(',');


S_Data26 = Load 'hdfs://zobbi01:9000/input/1/SSG26' using PigStorage(',') as (f0:chararray,f1:chararray,f2:chararray,f3:chararray,f4:chararray,f5:chararray,f6:chararray,f7:chararray,f8:chararray,f9:chararray,f10:chararray);
S_G026= group  S_Data26 by (f2);
S_G02626= foreach S_G026 generate COUNT (S_Data26) as count26:long, S_Data26.f0 as v0, S_Data26.f1 as v1,S_Data26.f2 as v2,S_Data26.f3 as v3,S_Data26.f4 as v4,S_Data26.f5 as v5,S_Data26.f6 as v6,S_Data26.f7 as v7,S_Data26.f8 as v8,S_Data26.f9 as v9,S_Data26.f10 as v10;
S_SG26= FILTER S_G02626 by (count26 >= 20);
S_SG026 = FOREACH S_SG26  GENERATE trial.NG_interval(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SG026 into 'hdfs://zobbi01:9000/output/1/SG42' using PigStorage(',');
S_SSG26= FILTER S_G02626 by (count26 < 20);
S_SSG026 = FOREACH S_SSG26  GENERATE trial.suppress(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SSG026 into 'hdfs://zobbi01:9000/output/1/SG_SP42' using PigStorage(',');


S_Data27 = Load 'hdfs://zobbi01:9000/input/1/SSG27' using PigStorage(',') as (f0:chararray,f1:chararray,f2:chararray,f3:chararray,f4:chararray,f5:chararray,f6:chararray,f7:chararray,f8:chararray,f9:chararray,f10:chararray);
S_G027= group  S_Data27 by (f2);
S_G02727= foreach S_G027 generate COUNT (S_Data27) as count27:long, S_Data27.f0 as v0, S_Data27.f1 as v1,S_Data27.f2 as v2,S_Data27.f3 as v3,S_Data27.f4 as v4,S_Data27.f5 as v5,S_Data27.f6 as v6,S_Data27.f7 as v7,S_Data27.f8 as v8,S_Data27.f9 as v9,S_Data27.f10 as v10;
S_SG27= FILTER S_G02727 by (count27 >= 20);
S_SG027 = FOREACH S_SG27  GENERATE trial.NG_interval(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SG027 into 'hdfs://zobbi01:9000/output/1/SG43' using PigStorage(',');
S_SSG27= FILTER S_G02727 by (count27 < 20);
S_SSG027 = FOREACH S_SSG27  GENERATE trial.suppress(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SSG027 into 'hdfs://zobbi01:9000/output/1/SG_SP43' using PigStorage(',');


S_Data28 = Load 'hdfs://zobbi01:9000/input/1/SSG28' using PigStorage(',') as (f0:chararray,f1:chararray,f2:chararray,f3:chararray,f4:chararray,f5:chararray,f6:chararray,f7:chararray,f8:chararray,f9:chararray,f10:chararray);
S_G028= group  S_Data28 by (f2);
S_G02828= foreach S_G028 generate COUNT (S_Data28) as count28:long, S_Data28.f0 as v0, S_Data28.f1 as v1,S_Data28.f2 as v2,S_Data28.f3 as v3,S_Data28.f4 as v4,S_Data28.f5 as v5,S_Data28.f6 as v6,S_Data28.f7 as v7,S_Data28.f8 as v8,S_Data28.f9 as v9,S_Data28.f10 as v10;
S_SG28= FILTER S_G02828 by (count28 >= 20);
S_SG028 = FOREACH S_SG28  GENERATE trial.NG_interval(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SG028 into 'hdfs://zobbi01:9000/output/1/SG44' using PigStorage(',');
S_SSG28= FILTER S_G02828 by (count28 < 20);
S_SSG028 = FOREACH S_SSG28  GENERATE trial.suppress(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SSG028 into 'hdfs://zobbi01:9000/output/1/SG_SP44' using PigStorage(',');


S_Data29 = Load 'hdfs://zobbi01:9000/input/1/SSG29' using PigStorage(',') as (f0:chararray,f1:chararray,f2:chararray,f3:chararray,f4:chararray,f5:chararray,f6:chararray,f7:chararray,f8:chararray,f9:chararray,f10:chararray);
S_G029= group  S_Data29 by (f2);
S_G02929= foreach S_G029 generate COUNT (S_Data29) as count29:long, S_Data29.f0 as v0, S_Data29.f1 as v1,S_Data29.f2 as v2,S_Data29.f3 as v3,S_Data29.f4 as v4,S_Data29.f5 as v5,S_Data29.f6 as v6,S_Data29.f7 as v7,S_Data29.f8 as v8,S_Data29.f9 as v9,S_Data29.f10 as v10;
S_SG29= FILTER S_G02929 by (count29 >= 20);
S_SG029 = FOREACH S_SG29  GENERATE trial.NG_interval(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SG029 into 'hdfs://zobbi01:9000/output/1/SG45' using PigStorage(',');
S_SSG29= FILTER S_G02929 by (count29 < 20);
S_SSG029 = FOREACH S_SSG29  GENERATE trial.suppress(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SSG029 into 'hdfs://zobbi01:9000/output/1/SG_SP45' using PigStorage(',');


S_Data30 = Load 'hdfs://zobbi01:9000/input/1/SSG30' using PigStorage(',') as (f0:chararray,f1:chararray,f2:chararray,f3:chararray,f4:chararray,f5:chararray,f6:chararray,f7:chararray,f8:chararray,f9:chararray,f10:chararray);
S_G030= group  S_Data30 by (f2);
S_G03030= foreach S_G030 generate COUNT (S_Data30) as count30:long, S_Data30.f0 as v0, S_Data30.f1 as v1,S_Data30.f2 as v2,S_Data30.f3 as v3,S_Data30.f4 as v4,S_Data30.f5 as v5,S_Data30.f6 as v6,S_Data30.f7 as v7,S_Data30.f8 as v8,S_Data30.f9 as v9,S_Data30.f10 as v10;
S_SG30= FILTER S_G03030 by (count30 >= 20);
S_SG030 = FOREACH S_SG30  GENERATE trial.NG_interval(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SG030 into 'hdfs://zobbi01:9000/output/1/SG46' using PigStorage(',');
S_SSG30= FILTER S_G03030 by (count30 < 20);
S_SSG030 = FOREACH S_SSG30  GENERATE trial.suppress(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SSG030 into 'hdfs://zobbi01:9000/output/1/SG_SP46' using PigStorage(',');


S_Data31 = Load 'hdfs://zobbi01:9000/input/1/SSG31' using PigStorage(',') as (f0:chararray,f1:chararray,f2:chararray,f3:chararray,f4:chararray,f5:chararray,f6:chararray,f7:chararray,f8:chararray,f9:chararray,f10:chararray);
S_G031= group  S_Data31 by (f2);
S_G03131= foreach S_G031 generate COUNT (S_Data31) as count31:long, S_Data31.f0 as v0, S_Data31.f1 as v1,S_Data31.f2 as v2,S_Data31.f3 as v3,S_Data31.f4 as v4,S_Data31.f5 as v5,S_Data31.f6 as v6,S_Data31.f7 as v7,S_Data31.f8 as v8,S_Data31.f9 as v9,S_Data31.f10 as v10;
S_SG31= FILTER S_G03131 by (count31 >= 20);
S_SG031 = FOREACH S_SG31  GENERATE trial.NG_interval(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SG031 into 'hdfs://zobbi01:9000/output/1/SG47' using PigStorage(',');
S_SSG31= FILTER S_G03131 by (count31 < 20);
S_SSG031 = FOREACH S_SSG31  GENERATE trial.suppress(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10);
STORE S_SSG031 into 'hdfs://zobbi01:9000/output/1/SG_SP47' using PigStorage(',');
