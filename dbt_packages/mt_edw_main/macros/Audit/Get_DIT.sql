/*
      This Macro will get the SystemID which is being used for Populating table_logging Table. This will also create Meta Date Table for if new Model being executed, and
      also Update the Source table from source meta data table.
      There is relation between table_metadateloaddt + table_logging Table based on MATADATELOADDT.SID(SOURCE ID)=table_logging.LID Link id
      Author :
      Creation Date : 12/15/2021
      Change History
      =========================================================================================================
      Updateion Date                  Updated By                  Comment
      ==========================================================================================================
      12/15/2021                      Pradeep                     Inital Draft
      01/21/2022                      Pradeep                     Updated all Entity Name with Veriables
      01/23/2022                      Pradeep                     Added Updated Logic to Metadata table for Source system and delete metadata columns
      02/22/2022                      Pradeep                     Solving table lock functionality
      
      Paramaters :
      Sname : System_name 
      DestTbl : Destination Table
      Sview :Source View 
*/

 {% macro Get_DIT(Sname,DestTbl,Sview) %}
 {% set new_query_tag = model.name+"_tmp1" %}
 {% set MetadataTbl = "Metadateload_"+model.name+"_tmp" %}

     {% set table_query_update %}
                                          CREATE OR REPLACE TABLE {{var('db_srcviewdb')}}.{{MetadataTbl}}
                                          AS SELECT * FROM CONSOLIDATED.{{var('table_metadateloaddt')}}  WHERE  SYSTEM_NAME='{{Sname}}'  AND DSTTBLNAME='{{DestTbl}}';


                                           
                                          UPDATE {{var('db_srcviewdb')}}.{{MetadataTbl}}  a SET DEST_CHANGE_KEY=B.DEST_CHANGE_KEY,SOURCE_CHANGE_TBL=B.SOURCE_CHANGE_TBL,SOURCE_CHANGE_KEY=B.SOURCE_CHANGE_KEY
                                                      ,SCDTYPE=B.SCDTYPE,SRCTBLCONDITION=B.SRCTBLCONDITION,SOURCE_DRIVING_TBL=B.SRCTBLNAME
                                                      FROM {{var('db_srcviewdb')}}.{{var('view_metadataupdview')}} B WHERE A.SYSTEM_NAME=B.SYSTEMNAME AND A.DSTTBLNAME=B.TBLNAME      
                                                      AND B.SYSTEMNAME='{{Sname}}' AND B.TBLNAME='{{DestTbl}}';  
                                          
       {% endset %}

       {%- set resultupdate = run_query(table_query_update) -%}


    {% set table_query %}
          SELECT DISTINCT NVL(SID,0) AS SID FROM {{var('db_destschema')}}.{{var('table_metadateloaddt')}}  WHERE  SYSTEM_NAME='{{Sname}}'  AND DSTTBLNAME='{{DestTbl}}'
    {% endset %}

    {% if execute %}
          {%- set result = run_query(table_query).columns[0].values()[0] -%}
            {%- set  result = result|replace("'", '"')|replace("('", '')|replace("',)", '') -%}
             {% if result|length < 1 %}
                        {% set tbl_query %}
                                       
                                          INSERT INTO {{var('db_destschema')}}.{{var('table_metadateloaddt')}} (SYSTEM_NAME,SRCTBLNAME,SRCSCHEMA,DSTTBLNAME,DSTSCHEMA,ISACTIVE,LOADMETADTS,ISSQORDAY,SCDTYPE,UPDATEDDT)
                                                      VALUES('{{Sname}}','{{Sview}}','EDW_DBT_VIEWS','{{DestTbl}}','CONSOLIDATED','Y','1900-01-01','S','SCD2',CURRENT_DATE);
                                          
                                          CREATE OR REPLACE TABLE {{var('db_srcviewdb')}}.{{MetadataTbl}}
                                          AS SELECT * FROM CONSOLIDATED.METADATELOADDT WHERE  SYSTEM_NAME='{{Sname}}'  AND DSTTBLNAME='{{DestTbl}}';
                                          
                                          UPDATE {{var('db_srcviewdb')}}.{{MetadataTbl}}  a SET DEST_CHANGE_KEY=B.DEST_CHANGE_KEY,SOURCE_CHANGE_TBL=B.SOURCE_CHANGE_TBL,SOURCE_CHANGE_KEY=B.SOURCE_CHANGE_KEY,
                                                      SCDTYPE=B.SCDTYPE,SRCTBLCONDITION=B.SRCTBLCONDITION,SOURCE_DRIVING_TBL=B.SRCTBLNAME
                                                      FROM {{var('db_srcviewdb')}}.{{var('view_metadataupdview')}} B WHERE A.SYSTEM_NAME=B.SYSTEMNAME AND A.DSTTBLNAME=B.TBLNAME      
                                                      AND B.SYSTEMNAME='{{Sname}}' AND B.TBLNAME='{{DestTbl}}';    
                                         
                        {% endset %}
                       
                         {% set tbl_query1 %}
                                 SELECT DISTINCT NVL(SID,0) AS SID FROM {{var('db_destschema')}}.{{var('table_metadateloaddt')}}  WHERE  SYSTEM_NAME='{{Sname}}'  AND DSTTBLNAME='{{DestTbl}}'
                         {% endset %}
                         {%- set result1 = run_query(tbl_query) -%}
                         {%- set result = run_query(tbl_query1).columns[0].values()[0] -%}
                         {{return(result)}}

             {% else %}
                   {{return(result)}}
             {% endif %}         
      {% else %}
                {{return( false) }}
    {% endif %}
{% endmacro %}
 