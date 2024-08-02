/*
      This Macro will create  procedure will Insert the rows from source view to Destination table based on last metadata load of metadataload from table_metadateloaddt Table
      Author :
      Creation Date : 12/15/2021
      Change History
      =========================================================================================================
      Updateion Date                  Updated By                  Comment
      ==========================================================================================================
      12/15/2021                      Pradeep                     Inital Draft

      
      
*/
{% macro Create_Procedure_objectrs() %}

USE SCHEMA CONSOLIDATED;


CREATE OR REPLACE PROCEDURE "DYNAMICVIEWFORTABLE"("schemaname" VARCHAR(255))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '

var varschema=schemaname;
  var getMetaDateSQL  =" SELECT concat(''CREATE OR REPLACE VIEW "+varschema+".VW_'', TABLE_NAME,'' COPY GRANTS AS SELECT '', VIEWCOLNAME ,'' FROM "+varschema+".'',TABLE_NAME , '' WHERE CURR_RCRD_FLAG =''''Y''''''  ) AS VIEWSCRIPT "
      getMetaDateSQL +="  FROM (  SELECT TABLE_NAME,listagg(COLUMN_NAME,'','') within group (order by ORDINAL_POSITION) AS VIEWCOLNAME "
      getMetaDateSQL +=" FROM (SELECT TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=''"+varschema+"'' AND (TABLE_NAME LIKE ''EDW_%'' AND TABLE_NAME NOT LIKE ''%VW%'' "
      getMetaDateSQL +=" AND TABLE_NAME NOT IN (select distinct DSTTBLNAME from "+varschema+".METADATELOADDT where ISACTIVE =''X'') )) TAB GROUP BY TABLE_NAME  ) A WHERE TABLE_NAME NOT IN(''EDW_MAPPING_INVENTORY'',''EDW_MAPPING_INVENTORY_1'',''EDW_SOURCE_SYSTEM'')"
  
//return getMetaDateSQL;
    var stmt_md = snowflake.createStatement( {sqlText: getMetaDateSQL} );
    var resultSetmd= stmt_md.execute();
   
   while (resultSetmd.next())  {
         getNewViewload = resultSetmd.getColumnValue(1);
        var stmt_md = snowflake.createStatement( {sqlText: getNewViewload} );
        var resultSetmd1= stmt_md.execute();
         
       }
  
    return ''EXECUTED''
 
  ';

CREATE OR REPLACE PROCEDURE DYNAMICVIEWFORTABLE_BYNAME ("schemaname" VARCHAR(255) , "name"  VARCHAR(255) )
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '



var varschema=schemaname;
  var getMetaDateSQL  =" SELECT concat(''CREATE OR REPLACE VIEW "+varschema+".VW_'', TABLE_NAME,'' COPY GRANTS AS SELECT '', VIEWCOLNAME ,'' FROM "+varschema+".'',TABLE_NAME , '' WHERE CURR_RCRD_FLAG =''''Y''''''  ) AS VIEWSCRIPT "
      getMetaDateSQL +="  FROM (  SELECT TABLE_NAME,listagg(COLUMN_NAME,'','') within group (order by ORDINAL_POSITION) AS VIEWCOLNAME "
      getMetaDateSQL +=" FROM (SELECT TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=''"+varschema+"'' AND TABLE_NAME like ''%"+name+"%'' AND (TABLE_NAME LIKE ''EDW_%'' AND TABLE_NAME NOT LIKE ''%VW%'' "
      getMetaDateSQL +=" AND TABLE_NAME NOT IN (select distinct DSTTBLNAME from "+varschema+".METADATELOADDT where ISACTIVE =''X'') )) TAB GROUP BY TABLE_NAME  ) A WHERE TABLE_NAME NOT IN(''EDW_MAPPING_INVENTORY'',''EDW_MAPPING_INVENTORY_1'',''EDW_SOURCE_SYSTEM'')"
  
//return getMetaDateSQL;
    var stmt_md = snowflake.createStatement( {sqlText: getMetaDateSQL} );
    var resultSetmd= stmt_md.execute();
   
   while (resultSetmd.next())  {
        getNewViewload = resultSetmd.getColumnValue(1);
        var stmt_md = snowflake.createStatement( {sqlText: getNewViewload} );
        try {
           var resultSetmd1= stmt_md.execute();
        }catch (err)  {}
         
    }
  
    return ''EXECUTED''

  ';
  
CREATE OR REPLACE PROCEDURE "SP_EDW_CALENDAR"("schemaname" VARCHAR(255))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '

var return_value="";
var getInsStmnt ="";
var varschema=schemaname;

var truncCalTblSQL = "TRUNCATE TABLE "+varschema+".EDW_CALENDAR";
var stmt_md = snowflake.createStatement( {sqlText: truncCalTblSQL} );
var resultSetmd= stmt_md.execute();


var insertSQL = " INSERT INTO "+varschema+".EDW_CALENDAR (SRC_SYS_KEY,CLNDR_KEY,SRC_RCRD_CREATE_DTE,SRC_RCRD_CREATE_USERID,"
 insertSQL += "SRC_RCRD_UPD_DTE, SRC_RCRD_UPD_USERID, RCRD_HASH_KEY, DEL_FROM_SRC_FLAG, ETL_INS_PID, ETL_INS_DTE, ETL_UPD_PID, ETL_UPD_DTE,"
 insertSQL += "ZONE3_LOD_DTE, EFF_DTE, EXPR_DTE, CURR_RCRD_FLAG, ORP_RCRD_FLAG, CLNDR_ID, DY_DTE, CLNDR_MTH_ID, CLNDR_QTR_ID, "
 insertSQL += "CLNDR_WK_ID, CLNDR_YR_ID, DY_ID, ETL_BATCH_ID, FMTH_ID, FQTR_ID, FWK_ID, FYR_ID, ABSOLUTE_DY_NBR, ABSOLUTE_WK_NBR, "
 insertSQL += "ABSOLUTE_MTH_NBR, ABSOLUTE_QTR_NBR, DY_IN_WK_NBR, DY_IN_WK_NAME, JULIAN_DY_NBR, CLNDR_WK_NBR, CLNDR_MTH_NBR, CLNDR_MTH_NAME, "
 insertSQL += "CLNDR_QTR_NBR, CLNDR_QTR_NAME, CLNDR_DY_IN_MTH_NBR, IS_FIRST_DY_IN_CLNDR_MTH_FLAG, IS_LAST_DY_IN_CLNDR_MTH_FLAG, "
 insertSQL += "WK_BEGIN_DTE, WK_END_DTE, FMTH_BEGIN_DTE, FMTH_END_DTE, FQTR_BEGIN_DTE, FQTR_END_DTE, FWK_NBR, FWK_CD, FMTH_NBR, FMTH_CD,"
 insertSQL += "FMTH_NAME, FMTH_SHORT_NAME,FQTR_CD, FDY_IN_MTH_NBR, FSCL_DYS_REMAIN_IN_MTH, FDY_IN_QTR_NBR, FSCL_DYS_REMAIN_IN_QTR,"
 insertSQL += "FDY_IN_YR_NBR, FSCL_DYS_REMAIN_IN_YR,FWK_IN_MTH_NBR,IS_WK_DY_FLAG,IS_WEEKEND_FLAG,IS_FIRST_DY_OF_FWK_FLAG,IS_LAST_DY_OF_FWK_FLAG,"
 insertSQL += "IS_FIRST_DY_OF_FMTH_FLAG, IS_LAST_DY_OF_FMTH_FLAG, IS_FIRST_DY_OF_FQTR_FLAG, IS_LAST_DY_OF_FQTR_FLAG,IS_FIRST_DY_OF_FYR_FLAG,"
 insertSQL += "IS_LAST_DY_OF_FYR_FLAG, SEASON_NAME, HOLIDAY_NAME, HOLIDAY_SEASON_NAME, HOLIDAY_OBSRV_NAME, SPCL_EVNT_NAME,FYR_BEGIN_DTE,"
 insertSQL += "FYR_END_DTE, FQTR_NBR, FQTR_NAME, FWK_IN_QTR) "
 insertSQL += "SELECT ''ALL'', DY_ID, CURRENT_TIMESTAMP::TIMESTAMP_NTZ,NULL,CURRENT_TIMESTAMP::TIMESTAMP_NTZ,NULL,"
 insertSQL += "MD5(DY_ID),''N'' as DEL_FROM_SRC_FLAG,''SP_EDW_CALENDAR'',CURRENT_TIMESTAMP::TIMESTAMP_NTZ, "
 insertSQL += "''SP_EDW_CALENDAR'',CURRENT_TIMESTAMP::TIMESTAMP_NTZ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ as ZONE3_LOD_DTE, "
 insertSQL += "CURRENT_TIMESTAMP::TIMESTAMP_NTZ as EFF_DTE, TRY_to_date(''9999.12.31'', ''YYYY.MM.DD'') as EXPR_DTE, "
 insertSQL += "''Y'' as CURR_RCRD_FLAG,''N'' as ORP_RCRD_FLAG, DY_ID	,DY_DTE	,CLNDR_MTH_ID, CLNDR_QTR_ID, CLNDR_WK_ID, "
 insertSQL += "CLNDR_YR_ID, DY_ID, NULL as ETL_BATCH_ID, FMTH_ID	,FQTR_ID,FWK_ID	,FYR_ID	,ABSOLUTE_DY_NBR,ABSOLUTE_WK_NBR,"
 insertSQL += "ABSOLUTE_MTH_NBR	,ABSOLUTE_QTR_NBR	,DY_IN_WK_NBR	,DY_IN_WK_NAME	,"
 insertSQL += "JULIAN_DY_NBR	,CLNDR_WK_NBR	,CLNDR_MTH_NBR	,CLNDR_MTH_NAME	,CLNDR_QTR_NBR	,CLNDR_QTR_NAME	,CLNDR_DY_IN_MTH_NBR,"
 insertSQL += "IS_FIRST_DY_IN_CLNDR_MTH_FLAG	,IS_LAST_DY_IN_CLNDR_MTH_FLAG	,WK_BEGIN_DTE	,WK_END_DTE	,FMTH_BEGIN_DTE	,FMTH_END_DTE,"
 insertSQL += "FQTR_BEGIN_DTE	,FQTR_END_DTE	,FWK_NBR	,FWK_CD	,FMTH_NBR	,FMTH_CD	,FMTH_NAME	,FMTH_SHORT_NAME,FQTR_CD,"
 insertSQL += "FDY_IN_MTH_NBR	,FSCL_DAYS_REMAINING_IN_MTH	,FDY_IN_QTR_NBR	,FSCL_DAYS_REMAINING_IN_QTR	,FDY_IN_YR_NBR	,"
 insertSQL += "FSCL_DAYS_REMAINING_IN_YR	,FWK_IN_MTH_NBR	,IS_WK_DY_FLAG	,IS_WEEKEND_FLAG	,IS_FIRST_DY_OF_FWK_FLAG	,"
 insertSQL += "IS_LAST_DY_OF_FWK_FLAG	,IS_FIRST_DY_OF_FMTH_FLAG	,IS_LAST_DY_OF_FMTH_FLAG	,IS_FIRST_DY_OF_FQTR_FLAG,"
 insertSQL += "IS_LAST_DY_OF_FQTR_FLAG	,IS_FIRST_DY_OF_FYR_FLAG	,IS_LAST_DY_OF_FYR_FLAG	,SEASON_NAME	,HOLIDAY_NAME,"
 insertSQL += "HOLIDAY_SEASON_NAME	,HOLIDAY_OBSERVED_NAME	,SPECIAL_EVENT_NAME	,FYR_BEGIN_DTE	,FYR_END_DTE	,FQTR_NBR	,"
 insertSQL += "FQTR_NAME	,FWK_IN_QTR	 FROM EDW_DBT_VIEWS.CALENDAR "

//return insertSQL    
       
var stmt_md1 = snowflake.createStatement( {sqlText: insertSQL} );
var resultSetmd1= stmt_md1.execute();

 return "EDW_CALENDAR Loaded";       
';

CREATE OR REPLACE PROCEDURE "SP_EDW_CODELOV"("schemaname" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
  // This procedure will Insert the rows from source view to Destination table based on last metadata load of metadataload from 
  // METADATELOADDT Table 
var v_schema=schemaname;

//Checking if this is a first load

if(v_schema != "ALL")
{ 
    var listofviewsstmt = `select distinct TABLE_NAME, SPLIT_PART(TABLE_NAME,''_'',1) from "INFORMATION_SCHEMA"."TABLES" 
                            where TABLE_NAME like ''%`+v_schema+`%_CODELOV_%VW''
                    and TABLE_SCHEMA =''EDW_DBT_VIEWS'' and TABLE_TYPE =''VIEW'' order by TABLE_NAME;`
  //return listofviewsstmt;
    var listofviewssql = snowflake.createStatement( {sqlText: listofviewsstmt} );
    var listofviews = listofviewssql.execute();

}
else
{ 
    var listofviewsstmt = `select distinct TABLE_NAME, SPLIT_PART(TABLE_NAME,''_'',1) from "INFORMATION_SCHEMA"."TABLES" 
                            where TABLE_NAME like ''%_CODELOV_%VW''
                    and TABLE_SCHEMA =''EDW_DBT_VIEWS'' and TABLE_TYPE =''VIEW'' order by TABLE_NAME;`
  //return listofviewsstmt;
    var listofviewssql = snowflake.createStatement( {sqlText: listofviewsstmt} );
    var listofviews = listofviewssql.execute();
}
    
while (listofviews.next())
{ 

   var v_schemaname  = listofviews.getColumnValue(2);
   var v_tblname = listofviews.getColumnValue(1);
  
   var cntsstmt = `select count(*) from "CONSOLIDATED"."EDW_CODE_LOV" where SRC_SYS_KEY =''`+v_schemaname+`''and ETL_INS_PID=''`+v_tblname+`'';`
   
   var cntssql = snowflake.createStatement( {sqlText: cntsstmt} );
   var cntexec = cntssql.execute();

    //return cntsstmt;
  while (cntexec.next())
   { 
     if(cntexec.getColumnValue(1) == 0)
   
   { 
    var insertstmtsql = `INSERT INTO "CONSOLIDATED"."EDW_CODE_LOV" (ENTITY_NM,SRC_SYS_KEY,RCRD_HASH_KEY,DEL_FROM_SRC_FLAG,ETL_INS_PID,
	ETL_INS_DTE,ETL_UPD_PID,ETL_UPD_DTE,EFF_DTE,EXPR_DTE,CURR_RCRD_FLAG,ORP_RCRD_FLAG,ZONE3_LOD_DTE,CD_LOV_LKEY,
	CD_NAME,CD_VAL,CD_DESC,LANG_KEY,NONPK_HASH_KEY)
    SELECT ENTITY_NM,SRC_SYS_KEY,RCRD_HASH_KEY,DEL_FROM_SRC_FLAG,ETL_INS_PID,
	ETL_INS_DTE,ETL_UPD_PID,ETL_UPD_DTE,EFF_DTE,EXPR_DTE,CURR_RCRD_FLAG,ORP_RCRD_FLAG,ZONE3_LOD_DTE,CD_LOV_LKEY,
	CD_NAME,CD_VAL,CD_DESC,LANG_KEY,NONPK_HASH_KEY FROM EDW_DBT_VIEWS.`+v_tblname+`;`
    //return insertstmtsql;
    var insertstmtstmt = snowflake.createStatement( {sqlText: insertstmtsql} );
    insertstmtres= insertstmtstmt.execute();
    
}
else
{ 
//This merge is for updating changed ones and inserting new ones
var merge1sql = `MERGE INTO "CONSOLIDATED"."EDW_CODE_LOV" tab USING EDW_DBT_VIEWS.`+v_tblname+ ` vw ON 
tab.RCRD_HASH_KEY = vw.RCRD_HASH_KEY and tab.SRC_SYS_KEY = vw.SRC_SYS_KEY and tab.SRC_SYS_KEY =''`+v_schemaname+`''
and tab.ETL_INS_PID =vw.ETL_INS_PID and tab.ETL_INS_PID=''`+v_tblname+`''
when MATCHED AND  tab.NONPK_HASH_KEY <> vw.NONPK_HASH_KEY AND tab.CURR_RCRD_FLAG=''Y''
THEN UPDATE set tab.ETL_UPD_PID =''SP_EDW_CODELOV'', tab.ETL_UPD_DTE = CURRENT_TIMESTAMP() , CURR_RCRD_FLAG=''N'',
EXPR_DTE =vw.EFF_DTE
WHEN NOT MATCHED THEN 
    INSERT (ENTITY_NM,SRC_SYS_KEY,RCRD_HASH_KEY,DEL_FROM_SRC_FLAG,ETL_INS_PID,
	ETL_INS_DTE,ETL_UPD_PID,ETL_UPD_DTE,EFF_DTE,EXPR_DTE,CURR_RCRD_FLAG,ORP_RCRD_FLAG,ZONE3_LOD_DTE,CD_LOV_LKEY,
	CD_NAME,CD_VAL,CD_DESC,LANG_KEY,NONPK_HASH_KEY)
    VALUES(vw.ENTITY_NM,vw.SRC_SYS_KEY,vw.RCRD_HASH_KEY,vw.DEL_FROM_SRC_FLAG,vw.ETL_INS_PID,
	vw.ETL_INS_DTE,vw.ETL_UPD_PID,vw.ETL_UPD_DTE,vw.EFF_DTE,vw.EXPR_DTE,vw.CURR_RCRD_FLAG,vw.ORP_RCRD_FLAG,vw.ZONE3_LOD_DTE,
    vw.CD_LOV_LKEY,	vw.CD_NAME, vw.CD_VAL, vw.CD_DESC, vw.LANG_KEY, vw.NONPK_HASH_KEY)`;

// return merge1sql;
    var merge1stmt = snowflake.createStatement( {sqlText: merge1sql} );
    merge1stmt.execute();
//return "first merge";

//This merge is for inserting updated records
var merge2sql = `MERGE INTO "CONSOLIDATED"."EDW_CODE_LOV" tab USING EDW_DBT_VIEWS.`+v_tblname+ ` vw ON 
tab.RCRD_HASH_KEY = vw.RCRD_HASH_KEY and tab.SRC_SYS_KEY = vw.SRC_SYS_KEY and tab.SRC_SYS_KEY =''`+v_schemaname+`''
and tab.ETL_INS_PID =vw.ETL_INS_PID and tab.ETL_INS_PID=''`+v_tblname+`''
and tab.CURR_RCRD_FLAG =''Y'' 
WHEN NOT MATCHED  THEN 
    INSERT (ENTITY_NM,SRC_SYS_KEY,RCRD_HASH_KEY,DEL_FROM_SRC_FLAG,ETL_INS_PID,
	ETL_INS_DTE,ETL_UPD_PID,ETL_UPD_DTE,EFF_DTE,EXPR_DTE,CURR_RCRD_FLAG,ORP_RCRD_FLAG,ZONE3_LOD_DTE,CD_LOV_LKEY,
	CD_NAME,CD_VAL,CD_DESC,LANG_KEY,NONPK_HASH_KEY)
    VALUES(vw.ENTITY_NM,vw.SRC_SYS_KEY,vw.RCRD_HASH_KEY,vw.DEL_FROM_SRC_FLAG,vw.ETL_INS_PID,
	vw.ETL_INS_DTE,vw.ETL_UPD_PID,vw.ETL_UPD_DTE,vw.EFF_DTE,vw.EXPR_DTE,vw.CURR_RCRD_FLAG,vw.ORP_RCRD_FLAG,vw.ZONE3_LOD_DTE,
    vw.CD_LOV_LKEY,	vw.CD_NAME, vw.CD_VAL, vw.CD_DESC, vw.LANG_KEY, vw.NONPK_HASH_KEY);`
    
    var merge2stmt = snowflake.createStatement( {sqlText: merge2sql} );
    merge2stmt.execute();
//return "second merge";    
    
//This merge is for soft deleting for deleted records
var merge3sql = ` MERGE INTO "CONSOLIDATED"."EDW_CODE_LOV" tab USING 
  ( select a.RCRD_HASH_KEY, a.CURR_RCRD_FLAG, a.cd_desc from "CONSOLIDATED"."EDW_CODE_LOV"   a left join  EDW_DBT_VIEWS.`+v_tblname+ ` b
 on a.RCRD_HASH_KEY = b.RCRD_HASH_KEY  and a.SRC_SYS_KEY = b.SRC_SYS_KEY and a.SRC_SYS_KEY =''`+v_schemaname+`''
 and a.ETL_INS_PID =b.ETL_INS_PID 
   where b.RCRD_HASH_KEY is NULL and a.CURR_RCRD_FLAG =''Y'' and a.SRC_SYS_KEY =''`+v_schemaname+`''
   and a.ETL_INS_PID=''`+v_tblname+`''
  ) vw ON 
tab.RCRD_HASH_KEY = vw.RCRD_HASH_KEY  
WHEN  MATCHED and tab.CURR_RCRD_FLAG =''Y''
THEN UPDATE set tab.ETL_UPD_PID =''SP_EDW_CODELOV'', tab.ETL_UPD_DTE = CURRENT_TIMESTAMP() , CURR_RCRD_FLAG=''N'',
EXPR_DTE =CURRENT_TIMESTAMP(),DEL_FROM_SRC_FLAG =''Y'';`

//return merge3sql;

    var merge3stmt = snowflake.createStatement( {sqlText: merge3sql} );
    merge3stmt.execute();    
//return "Third merge"; 
}
    }
}

return "executed";
 
';                      
{% endmacro %}