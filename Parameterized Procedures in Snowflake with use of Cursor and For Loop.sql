CREATE OR REPLACE PROCEDURE DEV_ESG_DB.DM_ESG_SCHEMA.PRC_ALL_RPT_MV("INITIATIVE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    COUNT_VIEW_NAME INTEGER;
    RPT_SQL_STATEMENT VARCHAR;
    TRUNCATE_RPT_SQL_STATEMENT VARCHAR;
    INSERT_RPT_SQL_STATEMENT VARCHAR;
    MV_SQL_STATEMENT VARCHAR;
BEGIN 

    --counting the number of views present with the INITIATIVE.
    COUNT_VIEW_NAME :=(SELECT COUNT(TABLE_NAME) 
                           FROM DEV_ESG_DB.INFORMATION_SCHEMA.VIEWS
                                WHERE TABLE_SCHEMA=''DM_ESG_SCHEMA'' AND TABLE_NAME LIKE (''VW_''||:INITIATIVE||''%''));

    --Checking if there are any views present with the INITITIVE''S                        
    IF (COUNT_VIEW_NAME = 0) 
        THEN 
        
            RETURN ''View has not been created yet.'';   --if there are no views present then returning the meassage no View present.
        
        ELSE
  
            LET C1 CURSOR FOR SELECT TABLE_NAME        --creating a on the Views present on the views.
                                FROM DEV_ESG_DB.INFORMATION_SCHEMA.VIEWS 
                                    WHERE TABLE_SCHEMA=''DM_ESG_SCHEMA'' AND TABLE_NAME LIKE (''VW_''||?||''%'');
        
            OPEN C1 USING(INITIATIVE);
            FOR record IN C1 DO
        
                --creating the RPT table from the view
                RPT_SQL_STATEMENT := ''CREATE TABLE IF NOT EXISTS DM_ESG_SCHEMA.RPT_''||SUBSTR(record.TABLE_NAME,4)||'' AS 
                                            SELECT * FROM DM_ESG_SCHEMA.''||record.TABLE_NAME||'''';
                EXECUTE IMMEDIATE RPT_SQL_STATEMENT;
        
                --truncate the RPT table
                TRUNCATE_RPT_SQL_STATEMENT:=''TRUNCATE TABLE DM_ESG_SCHEMA.RPT_''||SUBSTR(record.TABLE_NAME,4)||'''';
                EXECUTE IMMEDIATE TRUNCATE_RPT_SQL_STATEMENT;
        
                --insert from view to RPT table after truncate
                INSERT_RPT_SQL_STATEMENT:=''INSERT INTO DM_ESG_SCHEMA.RPT_''||SUBSTR(record.TABLE_NAME,4)||''
                                                SELECT *
                                                    FROM DM_ESG_SCHEMA.''||record.TABLE_NAME||'''';
                EXECUTE IMMEDIATE INSERT_RPT_SQL_STATEMENT;
                
                --If there are no Materialized views present creating the Materialized view.
                MV_SQL_STATEMENT :=''CREATE MATERIALIZED VIEW IF NOT EXISTS DM_ESG_SCHEMA.MV_'' ||SUBSTR(record.TABLE_NAME,4)||'' AS 
                                            SELECT * 
                                                FROM DM_ESG_SCHEMA.RPT_''||SUBSTR(record.TABLE_NAME,4)||'''';
                EXECUTE IMMEDIATE MV_SQL_STATEMENT;
             
            END FOR;
            
            CLOSE C1;
        
       RETURN ''RPT tables and MV materialized views are created if not exists.'';
        
    END IF;

EXCEPTION
    WHEN OTHER THEN 
        RETURN OBJECT_CONSTRUCT(''Error:'',SQLERRM);

END;
';