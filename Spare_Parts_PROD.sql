---------------------------------------MAIN---------------------------------------------------

CREATE OR REPLACE TABLE $DB_ANALYTICS.PS_AN_QLIK_OG_BITRANSFORM.T_SPP_INLFED 
(
KLART VARCHAR
);

INSERT INTO $DB_ANALYTICS.PS_AN_QLIK_OG_BITRANSFORM.T_SPP_INLFED (KLART) VALUES ('023'),('23');

CREATE OR REPLACE VIEW $DB_ACCESS.PS_AC_QLIK_OG_BITRANSFORM.V_SPP_INLFED AS 
(
SELECT * FROM $DB_ANALYTICS.PS_AN_QLIK_OG_BITRANSFORM.T_SPP_INLFED
);

--EQUIP
CREATE OR REPLACE VIEW $DB_ACCESS.PS_AC_QLIK_OG_BITRANSFORM.V_SPP_Equip AS 
(
WITH Equip_1 AS 
(
SELECT DISTINCT
TO_VARCHAR(EQ.EQUNR) AS "Equipment No",
TO_VARCHAR(EQ.EQUNR) AS EQUNR,
EQ.ILOAN AS ILOA_key,
EQ.TIDNR AS "HG/KKS",
IFF(TRIM(EQ.HEQUI) ='',NULL, TO_VARCHAR(EQ.HEQUI)) AS tmpParent,
TO_VARCHAR(EQ.EQUNR)||' '||TM.EQKTX AS tmpNodeDesc,
TM.EQKTX AS "Equipment Desc.",
EQ.IWERK as "Planning Plant"
FROM $DB_LANDING.PS102.EQUZ EQ
LEFT JOIN 
(
SELECT EQUNR,
        EQKTX 
FROM $DB_LANDING.PS102.EQKT
WHERE SPRAS='E'
) TM --Mapping table
ON EQ.EQUNR=TM.EQUNR
WHERE EQ.DATBI ='99991231' AND EQ.EQLFN ='001' AND EQ.IWERK IN ('1100','2800')
)
,Equip_2 AS                                     --EQUIP TO ILOA
(
SELECT Equip_1.*,
       IL.AUFNR,
       IL.ADRNR AS adress_key FROM Equip_1
LEFT JOIN
$DB_LANDING.PS102.ILOA IL
ON Equip_1.ILOA_key=IL.ILOAN
)
,Equip_3 AS                                     --EQUIP TO ADRC
(
SELECT Equip_2.* ,
       AD.COUNTRY,
       AD.COUNTRY as T_key, 
       AD.CITY1,
       AD.NAME1,
       AD.NAME2,
       AD.POST_CODE1,
       AD.SORT1,
       AD.STREET,
       AD.STR_SUPPL1,
       AD.TITLE
FROM Equip_2
LEFT JOIN
$DB_LANDING.PS102.ADRC AD
ON Equip_2.adress_key=AD.ADDRNUMBER
)
,Equip_4 AS                                     --EQUIP TO T005T
(
SELECT Equip_3.*, 
       T.LANDX,
       --T."Country",
       T.SPRAS
FROM Equip_3
LEFT JOIN
(
SELECT  LAND1 as T_key,
        LANDX,
        LANDX50 as "Country",
        SPRAS
FROM $DB_LANDING.PS102.T005T
WHERE SPRAS='E'
) T
ON Equip_3.T_key=T.T_key 
)
,Equip_5 AS                                     --EQUIP TO EQUI
(
SELECT DISTINCT Equip_4.*,
EQ.Model,
EQ."Material Assembly No."
FROM Equip_4
INNER JOIN
(                                            ---EQUI TABLE
SELECT TO_VARCHAR(EQUNR) AS EQUNR,
       TYPBZ AS Model,
       MATNR AS "Material Assembly No."
FROM $DB_LANDING.PS102.EQUI
WHERE EQTYP IN ('M','C','S','G','K', 'E', 'I','Q','U','D','')
)EQ
ON Equip_4.EQUNR=EQ.EQUNR
)SELECT * FROM EQUIP_5
);

-----------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW $DB_ACCESS.PS_AC_QLIK_OG_BITRANSFORM.V_SPP_Structure AS
(
WITH Structure_1 AS 
(SELECT DISTINCT EQUNR, 
	   tmpParent AS Parent,
	   tmpNodeDesc AS "Level",
	   EQUNR AS equip_limiter 
 FROM $DB_ACCESS.PS_AC_QLIK_OG_BITRANSFORM.V_SPP_Equip
 )
,Structure_2 AS 
(
SELECT SYS_CONNECT_BY_PATH("Level",'/') PATH,EQUNR,Parent,"Level",equip_limiter,LEVEL 
FROM Structure_1
START WITH Parent IS NULL
CONNECT BY PRIOR EQUNR=Parent
)
,Structure_3 AS
(
SELECT DISTINCT EQUNR,Parent,"Level",equip_limiter,INDEX, MAX(VALUE) OVER (PARTITION BY "Level", Parent) CT,PATH 
FROM Structure_2,TABLE(SPLIT_TO_TABLE(PATH, '/'))
ORDER BY "Level", Parent
),Structure_4 AS 
(
SELECT ST.EQUNR,ST.Parent,ST."Level",ST.equip_limiter,ST.PATH,C.INDEX AS DEPTH,C.value::string as PATHelement 
FROM Structure_3 ST,LATERAL FLATTEN(input=>split(PATH, '/')) C
),Structure_5 AS 
(
SELECT DISTINCT * FROM Structure_4
PIVOT(MAX(PATHelement) FOR DEPTH IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16)) AS HierarchyTable
(EQUNR,Parent,"Level",equip_limiter,PATH,Level1, Level2, Level3, Level4,Level5, Level6, Level7, Level8, Level9, Level10, Level11,Level12,Level13,Level14,Level15,Level16)
),Structure_6 AS 
(
SELECT Level1,
       COUNT(EQUNR) AS NodesInTree
FROM Structure_5
GROUP BY LEVEL1
),Structure_7 AS 
(
SELECT Structure_5.*,
       S.NodesInTree 
FROM Structure_5
INNER JOIN 
(
SELECT * FROM Structure_6
WHERE NodesInTree>=3
AND Level1 LIKE ('%Spare Part Products%')
AND NOT (Level1 LIKE ANY ('%wrong%','%error%','%fel%'))
) S
ON Structure_5.Level1=S.Level1
),Structure_8 AS
(
SELECT Structure_7.*,
ETM."Level 1 Model",
ETM."Top Model" 
FROM Structure_7
LEFT JOIN
(
SELECT DISTINCT
TO_VARCHAR(EQ.EQUNR)||' '||TM.EQKTX AS Level1,
EQ.TYPBZ as "Level 1 Model",
IFF(EQ."TYPBZ" LIKE 'SGT%',LEFT(regexp_replace(EQ."TYPBZ",' ',''),7),
IFF(EQ.TYPBZ='',EQ.TYPBZ, 'Other')) AS "Top Model"
FROM $DB_LANDING.PS102.EQUI EQ
LEFT JOIN
(
SELECT EQUNR,
       EQKTX 
FROM $DB_LANDING.PS102.EQKT
WHERE SPRAS='E'
) TM
ON EQ.EQUNR=TM.EQUNR
WHERE EQ.EQTYP IN ('M','C','S','G','K', 'E', 'I','Q','U','D','')
) ETM
ON Structure_7.Level1=ETM.Level1
)SELECT * FROM Structure_8 
);
---------------------------------------------------------------------------
--BOM_ITEMS
CREATE OR REPLACE VIEW $DB_ACCESS.PS_AC_QLIK_OG_BITRANSFORM.V_SPP_BOM_Items AS 
(
WITH BOM_1 AS 
(
SELECT DISTINCT 
TO_VARCHAR(EQUNR) AS EQUNR,
STLNR,
STLNR AS "Bill of Material"
FROM $DB_LANDING.PS102.EQST
WHERE TRY_TO_NUMBER(STLAN)=4 AND EQUNR IN (SELECT DISTINCT equip_limiter FROM $DB_ACCESS.PS_AC_QLIK_OG_BITRANSFORM.V_SPP_Structure)
),STPO_1 AS      --STPO TABLE
(
SELECT 
    DATUV,
    MENGE,
	STLTY, 
    STLNR,
	STLKN,
	IDNRK,
	ERSKZ,
	CLSZU,
	STPOZ,
    POTX1,
    POSNR,
    POSTP
FROM $DB_LANDING.PS102.STPO
WHERE STLNR IN (SELECT DISTINCT STLNR FROM BOM_1)
)
,STPO_2 AS 
(
SELECT STPO_1.DATUV AS "Valid From",
       STPO_1.MENGE AS "Installed Qty",
       STPO_1.POTX1 AS "Item text",
	   STPO_1.POSNR,
	   STPO_1.STLTY AS "BOM Category",
       STPO_1.STLNR AS "Bill of Material", 
	   STPO_1.STLKN AS "BOM Item Node Number",
	   STPO_1.IDNRK AS amount_key,-- kopplar tabell A909 m.hj.a. MATNR fältet 
	   STPO_1.IDNRK AS price_key, -- koppling till tabell MBEW  m.hj.a. MATNR fältet som visar Planned Future Price 1'
	   STPO_1.IDNRK AS date_key,  --koppling till tabell MARA m.hj.a. MATNR fältet 
	   STPO_1.IDNRK AS "Material",
	   TM.MAKTX AS "Material Desc.",
	   STPO_1.ERSKZ AS "Spare Part Indicator",
	   STPO_1.CLSZU AS "Classification Number",
	   STPO_1.STPOZ AS "Internal Counter STPO",
	   STPO_1.STLTY||STLNR||CLSZU AS STPO_Key, --Claes har lagt till. Möjligt att det inte är rätt. CLSZU ska kanske bytas ut mot något annat.
	   STPO_1.STLKN||STLNR||STLTY AS STAT_key
FROM STPO_1
LEFT JOIN 
(
SELECT MATNR,
       MAKTX 
FROM $DB_LANDING.PS102.MAKT
WHERE SPRAS ='E' 
)TM          --MtrlDesc Mapping table
ON STPO_1.IDNRK=TM.MATNR	 
WHERE STPO_1.STLTY='E' AND STPO_1.POSTP = 'L'
)
,STPO_3 AS 
(
SELECT STPO_2.*
FROM STPO_2
INNER JOIN 
(
SELECT "BOM Category",
"Bill of Material", 
"BOM Item Node Number", 
max("Internal Counter STPO") as "Internal Counter STPO"
FROM STPO_2 
GROUP BY  
"BOM Category",
"Bill of Material", 
"BOM Item Node Number"
) CT
ON STPO_2."BOM Category"=CT."BOM Category" AND 
STPO_2."Bill of Material"=CT."Bill of Material" AND 
STPO_2."BOM Item Node Number"=CT."BOM Item Node Number" AND
STPO_2."Internal Counter STPO"=CT."Internal Counter STPO"
)
,BOM_2 AS 
(
SELECT BOM_1.*,
       STPO_3."Valid From",
	   STPO_3."Installed Qty",
	   STPO_3."Item text",
	   STPO_3.POSNR,
	   STPO_3."BOM Category",
	   STPO_3."BOM Item Node Number",
	   STPO_3.amount_key,
	   STPO_3.price_key,
	   STPO_3.date_key,
	   STPO_3."Material",
	   STPO_3."Material Desc.",
	   STPO_3."Spare Part Indicator",
	   STPO_3."Classification Number",
	   STPO_3."Internal Counter STPO",
	   STPO_3.STPO_Key,
	   STPO_3.STAT_key     
FROM BOM_1
LEFT JOIN STPO_3
ON BOM_1."Bill of Material"=STPO_3."Bill of Material"
)
,STAS_1 AS --STAS TABLE
(
SELECT STLTY, 
       STLNR, 
       STLAL,
       STLKN,
       STASZ,
       LKENZ
FROM
$DB_LANDING.PS102.STAS
WHERE STLNR IN (SELECT DISTINCT "Bill of Material" FROM BOM_2)--NEED TO CHECK
)
,STAT_2 AS 
(
SELECT
     STLTY AS "BOM Category", 
     STLNR AS "Bill of Material", 
     STLAL AS "Alternative BOM",
     STLKN AS "BOM Item Node Number",
     TRY_TO_NUMBER(STASZ) AS "Internal Counter STAS",
     LKENZ AS STAS_Deletion_Indicator,
     STLKN||STLNR||STLTY AS STAT_key
FROM STAS_1
WHERE STLTY= 'E'
)
,STAT_3 AS 
(
SELECT STAT_2.*
FROM STAT_2
INNER JOIN
(
SELECT "BOM Category", 
       "Bill of Material",
       "Alternative BOM", 
       "BOM Item Node Number",
       MAX("Internal Counter STAS") AS "Internal Counter STAS"
FROM STAT_2
GROUP BY "BOM Category", 
         "Bill of Material",
         "Alternative BOM", 
         "BOM Item Node Number"
) ST
ON STAT_2."BOM Category"=ST."BOM Category" AND
   STAT_2."Bill of Material"=ST."Bill of Material" AND
   STAT_2."Alternative BOM"=ST."Alternative BOM" AND
   STAT_2."BOM Item Node Number"=ST."BOM Item Node Number" AND
   STAT_2."Internal Counter STAS"=ST."Internal Counter STAS"
)
,BOM_3 AS 
(
SELECT BOM_2.*,
       STAT_3."Alternative BOM",
       STAT_3.STAS_Deletion_Indicator
FROM BOM_2
INNER JOIN 
(SELECT * FROM STAT_3 WHERE TRIM(STAS_Deletion_Indicator)='') STAT_3
ON BOM_2."BOM Category"=STAT_3."BOM Category" AND
BOM_2."Bill of Material"=STAT_3."Bill of Material" AND 
BOM_2."BOM Item Node Number"=STAT_3."BOM Item Node Number" AND
BOM_2.STAT_key=STAT_3.STAT_key
)
,BOM_4 AS
(
SELECT BOM_3.*,
       A9.DATAB AS "Valid From A909",
       A9.KNUMH AS key,
       A9.KSCHL AS "Condition type",
       A9.VKORG AS "Sales Organization" 
FROM BOM_3
LEFT JOIN 
(
SELECT * 
FROM $DB_LANDING.PS102.A909 
WHERE DATBI = '99991231' AND VKORG = '1100' AND KSCHL = 'PR00' 
)A9
ON BOM_3.amount_key=A9.MATNR
)
,BOM_5 AS 
(
SELECT BOM_4.*,
       KO.KBETR as "Amount-Price",
       KO.KUNNR as Land_key 
	   FROM BOM_4
LEFT JOIN
(SELECT * FROM $DB_LANDING.PS102.KONP WHERE KUNNR='') KO
ON BOM_4.key=KO.KNUMH
)
,BOM_6 AS
(
SELECT BOM_5.*,
       IO.CUOBJ as Nyckel,
       IO.KLART,
       IO.OBJEK as INOB_OBJEK
FROM BOM_5
LEFT JOIN 
(SELECT * FROM $DB_LANDING.PS102.INOB
WHERE TRY_TO_NUMBER(KLART)=23 AND OBJEK LIKE ('E%'))IO
ON BOM_5.STPO_Key=IO.OBJEK
)
,BOM_7 AS 
(
SELECT DISTINCT BOM_6.*,
	   AU."Attribute",
	   AU."SPC Qty (Recommended)"
FROM BOM_6
LEFT JOIN 
(
SELECT DISTINCT 
	   CASE WHEN TRY_TO_NUMBER(ATINN)='444' THEN 'Spare Parts Attribute 1 = Operation Consumables'
	        WHEN TRY_TO_NUMBER(ATINN)='409' THEN 'Spare Parts Attribute 2 = Operation Back-up'
			WHEN TRY_TO_NUMBER(ATINN)='524' THEN 'Spare Parts Attribute 3 = Maintenance consumable'
			WHEN TRY_TO_NUMBER(ATINN)='518' THEN 'Spare Parts Attribute 4 = Replacement Parts'
			WHEN TRY_TO_NUMBER(ATINN)='477' THEN 'Spare Parts Attribute 5 = Contingency Parts'
			WHEN TRY_TO_NUMBER(ATINN)='529' THEN 'Spare Parts Attribute 6 = Capital Spares'
			WHEN TRY_TO_NUMBER(ATINN)='16648' THEN 'Spare Parts Attribute 7'
			WHEN TRY_TO_NUMBER(ATINN)='16649' THEN 'Spare Parts Attribute 9'
		END AS "Attribute",
		ATFLV AS "SPC Qty (Recommended)",
		OBJEK AS Nyckel,
        KLART
FROM $DB_LANDING.PS102.AUSP
WHERE TRY_TO_NUMBER(KLART)=23 AND ATFLV>=1
) AU
ON BOM_6.KLART=AU.KLART AND BOM_6.Nyckel=AU.Nyckel
)
,BOM_8 AS 
(
SELECT  BOM_7.*,
       MB.LAEPR as "Date of the last price change",
       MB.BWKEY as "Plant", 
	   MB.LPLPR as "Current Planned Price",
       MB.STPRS as "Standard price", 
       MB.VERPR as "Moving Average Price/Periodic Unit Price",  
       MB.VPRSV as "Price control indicator", 
       MB.ZPLP1 as "Planned Future Price 1"
FROM BOM_7 
LEFT JOIN 
(SELECT * FROM $DB_LANDING.PS102.MBEW WHERE BWTAR='' AND BWKEY IN ('1100', '2800')) MB
ON BOM_7.price_key=MB.MATNR
)
,BOM_9 AS 
(
SELECT BOM_8.*,
       MA.MSTDV as "Date from which the X-distr.-chain material status is valid" 
FROM BOM_8
LEFT JOIN 
$DB_LANDING.PS102.MARA MA
ON BOM_8.date_key=MA.MATNR
)SELECT * FROM BOM_9
);





