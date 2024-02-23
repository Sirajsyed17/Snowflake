CREATE OR REPLACE TABLE STG_ESG_SCHEMA.FORECASTING_TEST AS
(
WITH CTE AS
(
SELECT COUNTRY,
  ROPU,
  LMIC,
  Population,
  Incidence_of_stroke_per_100001,
  Number_of_patients_per_hospital_per_year,
  (Incidence_of_stroke_per_100001/100000*Population) AS Number_of_acute_strokes,
  IFF(Number_of_acute_strokes/Number_of_patients_per_hospital_per_year < 1,1,Number_of_acute_strokes/Number_of_patients_per_hospital_per_year) AS Number_of_stroke_centers_Needed,
  Year,
  Hospitals 
  FROM LANDING_ESG_SCHEMA.LANDING_ANGELS_HOSPITALS 
),SLOPE_INTERCEPT AS
(
SELECT COUNTRY,
	   REGR_SLOPE(Hospitals,Year) as SLOPE,
	   REGR_INTERCEPT(Hospitals,Year) AS INTERCEPT 
FROM LANDING_ESG_SCHEMA.LANDING_ANGELS_HOSPITALS
GROUP BY COUNTRY,LMIC
)
,GENERATOR AS 
( 
  SELECT ROW_NUMBER() OVER (ORDER BY seq4()) + 2022 YEAR FROM table(generator(ROWCOUNT => 8))
),COUNTRY_YEAR AS
(
SELECT DISTINCT COUNTRY,LMIC,Population,Incidence_of_stroke_per_100001,Number_of_patients_per_hospital_per_year,Number_of_acute_strokes,Number_of_stroke_centers_Needed,GENERATOR.YEAR 
FROM CTE 
CROSS JOIN GENERATOR
)
, JOIN_1 AS 
(
SELECT 
  COUNTRY_YEAR.COUNTRY,
  COUNTRY_YEAR.LMIC,
  COUNTRY_YEAR.Population,
  COUNTRY_YEAR.Incidence_of_stroke_per_100001,
  COUNTRY_YEAR.Number_of_patients_per_hospital_per_year,
  COUNTRY_YEAR.YEAR,
  COUNTRY_YEAR.Number_of_acute_strokes,
  COUNTRY_YEAR.Number_of_stroke_centers_Needed,
  (SLOPE*COUNTRY_YEAR.YEAR+INTERCEPT) AS HOSPITALS 
FROM COUNTRY_YEAR LEFT JOIN SLOPE_INTERCEPT ON COUNTRY_YEAR.COUNTRY=SLOPE_INTERCEPT.COUNTRY
)
,UNION_P_F AS 
(SELECT COUNTRY,
LMIC,
Population,
Incidence_of_stroke_per_100001,
Number_of_patients_per_hospital_per_year,
Number_of_acute_strokes,Number_of_stroke_centers_Needed,
TO_CHAR(Year) AS YEAR,
Hospitals 
FROM CTE
UNION ALL
SELECT COUNTRY,
LMIC,
Population,
Incidence_of_stroke_per_100001,
Number_of_patients_per_hospital_per_year,
Number_of_acute_strokes,
Number_of_stroke_centers_Needed,
TO_CHAR(Year) AS YEAR,
ROUND(Hospitals) 
FROM JOIN_1
)
SELECT COUNTRY,
LMIC,
Population,
Incidence_of_stroke_per_100001,
Number_of_patients_per_hospital_per_year,
ROUND(Number_of_acute_strokes) AS Number_of_acute_strokes,
ROUND(Number_of_stroke_centers_Needed) AS Number_of_stroke_centers_Needed, 
YEAR, 
HOSPITALS
/*IFF(YEAR IN ('2023','2024','2025','2026','2027','2028','2029','2030'),
    IFF(HOSPITALS<Number_of_stroke_centers_Needed,HOSPITALS,
        IFF(LAG(HOSPITALS) OVER (ORDER BY COUNTRY,YEAR)<Number_of_stroke_centers_Needed,Number_of_stroke_centers_Needed,NULL)),HOSPITALS) AS HOSPITALS*/
FROM UNION_P_F
ORDER BY COUNTRY,YEAR
);


CREATE OR REPLACE PROCEDURE DEV_ESG_DB.STG_ESG_SCHEMA.TEST1234()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
try {

var SEQ_YEAR = `SELECT distinct Year from STG_ESG_SCHEMA.FORECASTING_TEST where year>=YEAR(CURRENT_DATE()) ORDER BY YEAR;`;

var SEQ_STATEMENT = snowflake.createStatement({ sqlText: SEQ_YEAR });
var CMD1 = SEQ_STATEMENT.execute();

 for (i = 1; i <= SEQ_STATEMENT.getRowCount(); i++)
 {
    CMD1.next()
    YEAR_ID = CMD1.getColumnValue(1)
    PREV_YEAR_ID=YEAR_I_D;-1
    var SELECT_STMT = `UPDATE STG_ESG_SCHEMA.FORECASTING_TEST FK
SET FK.HOSPITALS=FK1.HOSPITAL_FINAL
FROM 
(SELECT country,year,IFF(HOSPITALS<Number_of_stroke_centers_Needed,HOSPITALS,
             IFF(PREV_HOSP<Number_of_stroke_centers_Needed,Number_of_stroke_centers_Needed,PREV_HOSP)) AS HOSPITAL_FINAL
             FROM 
             (
SELECT country,YEAR,Number_of_stroke_centers_Needed,HospitalS,LAG(HOSPITALS) OVER (ORDER BY COUNTRY,YEAR) AS PREV_HOSP 
    from STG_ESG_SCHEMA.FORECASTING_TEST where Year IN (${YEAR_ID},${PREV_YEAR_ID})
             /* and  COUNTRY=''Argentina''*/
    ORDER BY COUNTRY,YEAR) 
    where year=${YEAR_ID}) FK1
WHERE FK.COUNTRY=FK1.COUNTRY AND FK.YEAR=FK1.YEAR;`;
 
    snowflake.execute({sqlText: SELECT_STMT});
 
    /*return SELECT_STMT;*/
 }

}
catch(err)
{
 throw "Failed:" + err;
}
';