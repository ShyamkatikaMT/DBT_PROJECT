{% macro get_updateloaddate(Sname,DestTable) %}

{% set MetadataTbl = "Metadateload_"+model.name+"_tmp" %}

{% set Sname1=Sname|replace('_BACK','') %}
  
UPDATE {{var('db_srcviewdb')}}.{{MetadataTbl}} SET ISSQORDAY='S', {{var('column_loadmetadts')}}=(SELECT NVL(MAX({{var('column_z3loddtm')}}),'1900-01-01') FROM {{var('db_destschema')}}.{{DestTable}} WHERE {{var('column_srcsyskey')}}='{{Sname1}}')
WHERE SYSTEM_NAME='{{Sname}}' AND DSTTBLNAME='{{DestTable}}';

UPDATE CONSOLIDATED.{{var('table_metadateloaddt')}} A SET A.LOADMETADTS=B.LOADMETADTS,A.SCDTYPE=B.SCDTYPE,
A.DEST_CHANGE_KEY=B.DEST_CHANGE_KEY,A.SOURCE_CHANGE_TBL=B.SOURCE_CHANGE_TBL,A.SOURCE_CHANGE_KEY=B.SOURCE_CHANGE_KEY,
A.SOURCE_DRIVING_TBL=B.SOURCE_DRIVING_TBL,A.SRCTBLCONDITION=B.SRCTBLCONDITION
FROM {{var('db_srcviewdb')}}.{{MetadataTbl}} B
WHERE A.SID=B.SID;

 DROP TABLE   {{var('db_srcviewdb')}}.{{MetadataTbl}};
        
{% endmacro %}