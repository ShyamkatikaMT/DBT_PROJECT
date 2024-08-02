SELECT 
MARA.MATNR AS PROD_KEY,
'SAPC11' AS {{var('column_srcsyskey')}},
TRY_TO_DATE(MARA.ERSDA,'YYYYMMDD') AS {{var('column_SRC_RCRD_CREATE_DTE')}},
MARA.ERNAM AS {{var('column_SRC_RCRD_CREATE_USERID')}},
TRY_TO_DATE(MARA.LAEDA,'YYYYMMDD') AS {{var('column_SRC_RCRD_UPD_DTE')}},
MARA.AENAM AS  {{var('column_SRC_RCRD_UPD_USERID')}},
md5(MARA.MATNR) AS {{var('column_rechashkey')}},
{{var('default_n')}}  AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ  AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
MARA.LOADDTS AS {{var('column_vereffdte')}},
TRY_to_date('9999.12.31', 'YYYY.MM.DD') AS {{var('column_verexpirydt')}},
{{var('default_y')}}  AS {{var('column_currrecflag')}},
{{var('default_n')}}  AS {{var('column_orprecflag')}},
MARA.LOADDTS AS {{var('column_z3loddtm')}},
MARA.MATNR AS PROD_ID,
MARA.WRKST AS BASE_PROD_ID,
MARA.RBNRM AS CAT_CD,
MAKT.MAKTX AS PROD_NAME,
MAKT.MAKTG AS PROD_DESC,
MARA.MFRPN AS MFGR_PROD_NBR,
MARA.NORMT AS IND_STD_DESC,
--MARA.EAN11 AS UPC_NBR,
case when length(trim(MARA.NUMTP)) <1 then {{var('default_mapkey')}}
when MARA.NUMTP IS NULL then {{var('default_mapkey')}} else MARA.NUMTP END
AS  EAN_CTGY_LKEY,
case when length(trim(MAKT.SPRAS)) <1 then {{var('default_mapkey')}} when MAKT.SPRAS IS NULL then {{var('default_mapkey')}} else MAKT.SPRAS end As  LANG_KEY,
case when length(trim(MARA.IPRKZ)) <1 then {{var('default_mapkey')}}
when MARA.IPRKZ IS NULL then {{var('default_mapkey')}}
else MARA.IPRKZ
end as LFCYL_STAT_LKEY,
case when length(trim(MARA.MSTAE)) <1 then {{var('default_mapkey')}}
when MARA.MSTAE IS NULL then {{var('default_mapkey')}}
else MARA.MSTAE
end as ENGNR_STAT_LKEY,
{{var('default_key')}} as MFG_PUR_LKEY,
case when length(trim(MARA.MTART)) <1 then {{var('default_mapkey')}}
when MARA.MTART IS NULL then {{var('default_mapkey')}}
else MARA.MTART
end as PROD_TYP_LKEY,
case when length(trim(MARA.MATKL)) <1 then {{var('default_mapkey')}}
when MARA.MATKL IS NULL then {{var('default_mapkey')}}
else MARA.MATKL
end as  MKT_SEG_LKEY,
{{var('default_key')}} AS PROD_GRP_LKEY,
{{var('default_key')}} AS CMDTY_CD_LKEY,
{{var('default_key')}} AS PKG_LKEY,
{{var('default_key')}} as PRODM_LKEY,
case when length(trim(MARA.SPART)) <1 then {{var('default_mapkey')}} when MARA.SPART IS NULL then {{var('default_mapkey')}} else MARA.SPART end As  BRAND_LKEY,
{{var('default_key')}} AS SUB_BRAND_LKEY,
{{var('default_key')}} PROD_DIV_KEY,
--MARA.BSTAT AS SESNLTY_TEXT,--EDWI-624
--TRY_TO_DATE(MARA.LIQDT,'YYYYMMDD') AS OBSLT_DTE,--EDWI-624
--TRY_TO_DATE(MARA.ERSDA,'YYYYMMDD') AS CREATE_DT,-----Not Found
--MARA.MEINS AS BASE_UOM_KEY,
case when length(trim(MARA.MEINS)) <1 then {{var('default_mapkey')}}
when MARA.MEINS IS NULL then {{var('default_mapkey')}}
else MARA.MEINS
end as BASE_UOM_KEY,
case when length(trim(MARA.VOLEH)) <1 then {{var('default_mapkey')}}
when MARA.VOLEH IS NULL then {{var('default_mapkey')}}
else MARA.VOLEH
end as  VOL_UOM_KEY,
--MARA.GEWEI AS WGT_UOM_KEY,
case when length(trim(MARA.GEWEI)) <1 then {{var('default_mapkey')}}
when MARA.GEWEI IS NULL then {{var('default_mapkey')}}
else MARA.GEWEI
end as  WGT_UOM_KEY,
--MARA.BSTME AS PUR_UOM_KEY,
case when length(trim(MARA.BSTME)) <1 then {{var('default_mapkey')}}
when MARA.BSTME IS NULL then {{var('default_mapkey')}}
else MARA.BSTME
end as  PUR_UOM_KEY,
MARA.HOEHE AS HGT,
MARA.LAENG AS LEN,
MARA.BREIT AS WDTH,
MARA.VOLUM AS VOL,
MARA.BRGEW AS WGT,
MARA.GROES AS DIM_TEXT,
case when MARA.TAKLV = 0 OR trim(MARA.TAKLV) = '' OR trim(MARA.TAKLV) is null then 'N' else 'Y' end AS TAX_FLAG,
{{var('default_key')}} as DESIGN_GRP_LKEY,
DIV0(MARMBASE.UMREZ,MARMBASE.UMREN) AS SLS_TO_BASE_UOM_CONV_VAL,
DIV0(MARMBOX.UMREZ,MARMBOX.UMREN) AS BOX_QTY,	
DIV0(MARMCAR.UMREZ,MARMCAR.UMREN) AS CTN_QTY,

DIV0(MARMBASE.UMREZ,MARMBASE.UMREN) AS PUR_TO_BASE_UOM_CONV_VAL,

{{var('default_key')}} AS PRIM_PLANT_LOC_KEY,
TRY_to_date(MARA.DATAB, 'YYYYMMDD') AS LAUNCH_DTE,
/*case when length(trim(MARA.MEINH)) <1 then {{var('default_mapkey')}}
when MARA.MEINH IS NULL then {{var('default_mapkey')}}
else MARA.MEINH
end as SLS_UOM_KEY*/ --not there in source table

--1.2 CHANGES:
MARA.ZGTIN As PROD_GLB_TRDG_CD,
--TDG42.PROFLD As DNGR_GOODS_PRFL_TEXT,
{{var('default_key')}} As CMDTY_CLS_LKEY,
{{var('default_key')}} As CMPNT_TYP_LKEY,
{{var('default_key')}} As PROD_TXN_TYP_LKEY,
{{var('default_key')}} As SPARE_PART_CLS_LKEY,
{{var('default_key')}} As LOGSTC_UOM_KEY,
case when length(trim(MARA.KUNNR)) <1 then {{var('default_mapkey')}}
when MARA.KUNNR IS NULL then {{var('default_mapkey')}}
else MARA.KUNNR
end as COMPETITOR_CUST_KEY,
case when length(trim(MARA.SAISO)) <1 then {{var('default_mapkey')}}
when MARA.SAISO IS NULL then {{var('default_mapkey')}}
else MARA.SAISO
end as SEASON_CTGY_LKEY,
case when MARA.ZTRFLAG = 'X' then 'Y' else 'N' end As PROD_RECYCLABLE_FLAG,
MARA.FERTH As INSPCT_MEMO_NBR,
case when length(trim(MARA.EKWSL)) <1 then {{var('default_mapkey')}}
when MARA.EKWSL IS NULL then {{var('default_mapkey')}}
else MARA.EKWSL
end as PUR_VAL_LKEY,
case when length(trim(MARA.BEGRU)) <1 then {{var('default_mapkey')}}
when MARA.BEGRU IS NULL then {{var('default_mapkey')}}
else MARA.BEGRU
end as AUTH_GRP_LKEY,
case when length(trim(MARA.LABOR)) <1 then {{var('default_mapkey')}}
when MARA.LABOR IS NULL then {{var('default_mapkey')}}
else MARA.LABOR
end as LABORATORY_WORKER_USER_KEY,
case when length(trim(MARA.STOFF)) <1 then {{var('default_mapkey')}}
when MARA.STOFF IS NULL then {{var('default_mapkey')}}
else MARA.STOFF
end as HAZARDOUS_PROD_LKEY,
MARA.ERGEW As ALLOW_PKG_WGT2,
MARA.ERVOL As ALLOW_PKG_VOL,
MARA.GEWTO As HDLNG_UNIT_TLRNCE_WGT,
MARA.VOLTO As HDLNG_UNIT_TLRNCE_VOL,
case when length(trim(MARA.VABME)) <1 then {{var('default_mapkey')}}
when MARA.VABME IS NULL then {{var('default_mapkey')}}
else MARA.VABME
end as PUR_UOM_VARIATION_ACTV_LKEY,
case when MARA.KZREV = 'X' then 'Y' else 'N' end As RVN_LVL_ASGN_FLAG,
case when MARA.KZKFG = 'X' then 'Y' else 'N' end As CONFIGURABLE_MATL_FLAG,
{{var('default_key')}} As ABC_CLS_LKEY,
{{var('default_key')}} As PROD_DOMAIN_LKEY,
{{var('default_key')}} As PROD_FAMILY_LKEY,
MARA.WESCH As GR_SLIPS_CNT,
MARA.STFAK As PAL_STACK_CNT,
case when length(trim(MARA.MAGRV)) <1 then {{var('default_mapkey')}}
when MARA.MAGRV IS NULL then {{var('default_mapkey')}}
else MARA.MAGRV
end as PKG_MATL_GRP_LKEY,
TRY_TO_DATE(MARA.LIQDT,'YYYYMMDD') As PROD_DEL_DTE,
MARA.MHDRZ As MIN_SHELF_LIFE_DYS_CNT,
MARA.MHDHB As TOT_SHELF_LIFE_DYS_CNT,
MARA.MHDLP As PROD_REMAIN_SHELF_LIFE_PCT,
MARA.INHAL As PROD_NET_CONTENT_UOM_QTY,
try_to_number(MARA.INHME) As CONTENT_UNIT_QTY,
MARA.INHBR As PROD_GRS_CONTENT_UOM_QTY,
case when MARA.KZUMW = 'X' then 'Y' else 'N' end As ENVR_RELVNT_PROD_FLAG,
case when length(trim(MARA.KOSCH)) <1 then {{var('default_mapkey')}}
when MARA.KOSCH IS NULL then {{var('default_mapkey')}}
else MARA.KOSCH
end as PROD_ALLOC_PROC_LKEY,
MARA.NRFHG As PROD_DISC_APPBL,
case when MARA.IHIVI = 'X' then 'Y' else 'N' end As PROD_HIGHLY_VISCOUS_FLAG,
MARA.SERLV As PROD_UNIQUE_SERL_NBR_LVL,
case when MARA.KZGVH = 'X' then 'Y' else 'N' end As CLOSED_PKG_MATL_FLAG,
case when MARA.XGCHP = 'X' then 'Y' else 'N' end As APRV_BATCH_RCRD_FLAG,
case when MARA.KZEFF = 'X' then 'Y' else 'N' end As ASSIGN_EFFECTIVITY_PARM_FLAG,
case when length(trim(MARA.COMPL)) <1 then {{var('default_mapkey')}}
when MARA.COMPL IS NULL then {{var('default_mapkey')}}
else MARA.COMPL
end as PRODTN_STG_CLS_LKEY,
case when MARA.MATFI = 'X' then 'Y' else 'N' end As LOCKED_MATL_FLAG,
MARA.MAXC As PKG_MATL_MAX_CAPACITY,
MARA.MAXC_TOL As HU_TLRNCE_LIMIT,
--MARA.MAXL As PKG_MATL_MAX_LEN,
MARA.MAXB As PKG_MATL_MAX_WDTH,
MARA.MAXH As PKG_MATL_MAX_HGT,
MARA.QQTIME As PROD_QURNTN_PERD,
MARA._BEV1_LULEINH As PROD_LOD_UNIT_QTY,
MARA._VSO_R_PAL_OVR_D As PKG_MATL_PERMISSIBLE_DEPTH,
MARA._VSO_R_PAL_OVR_W As PKG_MATL_PERMISSIBLE_WDTH,
MARA._VSO_R_PAL_B_HT As PKG_MATL_MAX_STACK_HGT,
MARA._VSO_R_PAL_MIN_H As PKG_MATL_MIN_STACK_HGT,
MARA._VSO_R_TOL_B_HT As STACK_HGT_TLRNCE_PCT,
MARA._VSO_R_NO_P_GVH As PKM_CLOSED_MATL_CNT,
MARA.COLOR_ATINN As COLR_ITRL_CHAR_NBR,
MARA.SIZE1_ATINN As MAIN_SZ_ITRL_CHAR_NBR,
MARA.SIZE2_ATINN As SECOND_SZ_ITRL_CHAR_NBR,
MARA.FIBER_PART1 As FIBER_SHARE_1_PCT,
MARA.FIBER_PART2 As FIBER_SHARE_2_PCT,
MARA.FIBER_PART3 As FIBER_SHARE_3_PCT,
MARA.FIBER_PART4 As FIBER_SHARE_4_PCT,
MARA.FIBER_PART5 As FIBER_SHARE_5_PCT,
case when MARA.ZNCDIFL = 'X' then 'Y' else 'N' end As NET_CONTENT_DCLR_FLAG,
MARA.ZHLDEPTH As HARDLINES_OUT_OF_BOX_DEPTH,
MARA.ZHLHGT As HARDLINES_OUT_OF_BOX_HGT,
MARA.ZHLWDTH As HARDLINES_OUT_OF_BOX_WDTH,
MARA.ZMTSMTO As DS_MTS_MTO_INDCTR,
MARA._SBDC11_COO_CERT As COO_CERTIFICATION,
case when MARA.ILOOS = 'X' then 'Y' else 'N' end As FFF_CLS_FLAG,
case when length(trim(MARA.VHART)) <1 then {{var('default_mapkey')}}
when MARA.VHART IS NULL then {{var('default_mapkey')}}
else MARA.VHART
end as PKG_MATL_TYP_LKEY,
MARA.FUELG As MAX_PKG_MATL_PCT,
MARA.NTGEW as NET_WGT,
TRY_to_date(MARA.MSTDE, 'YYYYMMDD') as CROSS_PLAN_MATL_DTE,
TRY_to_date(MARA.MSTDV, 'YYYYMMDD') as CROSS_DIST_CHN_MATL_DTE,
case when length(trim(MARA.RAUBE)) <1 then {{var('default_mapkey')}}
when MARA.RAUBE IS NULL then {{var('default_mapkey')}}
else MARA.RAUBE
end as STORG_COND_LKEY,

case when length(trim(MARA.VPSTA)) <1 then {{var('default_mapkey')}} when MARA.VPSTA IS NULL then {{var('default_mapkey')}} else MARA.VPSTA end as MTNC_STAT_LKEY,
case when length(trim(MARA.MBRSH)) <1 then {{var('default_mapkey')}} when MARA.MBRSH IS NULL then {{var('default_mapkey')}} else MARA.MBRSH end as IND_SECTOR_LKEY,
MARA.PRDHA AS DNGR_GOODS_PRFL_TEXT,
case when length(trim(MARA.MEABM)) <1 then {{var('default_mapkey')}} when MARA.MEABM IS NULL then {{var('default_mapkey')}} else MARA.MEABM end as SZ_UOM_KEY,
MARA.MFRNR AS PROD_MFGR_NAME,
case when MARA.KZNFM = 'X' then 'Y' else 'N' end AS FOLLOW_UP_PROD_FLAG,
case when MARA.KZKUP = 'X' then 'Y' else 'N' end  AS CO_PROD_FLAG,
case when MARA.LVORM = 'X' then 'Y' else 'N' end  AS DEL_FLAG,

MARA.ZEIFO AS DOC_PAGE_FMT,
TRY_to_date(MARA.ERSDA, 'YYYYMMDD') as PROD_CREATE_DTE,
MARA.ANP AS ANP_CD,
case when length(trim(MARA.EXTWG)) <1 then {{var('default_mapkey')}} when MARA.EXTWG IS NULL then {{var('default_mapkey')}} else MARA.EXTWG end as EXTNL_PROD_GRP_LKEY,
MARA.ZPEGH AS HORIZONTAL,
MARA.BLANZ AS DOC_SHEET_CNT,
MARA.ZPEGV AS VERTICAL,
MARA.FORMT AS PROD_MEMO_PAGE_FMT,
case when length(trim(MARA.MSTAV)) <1 then {{var('default_mapkey')}} when MARA.MSTAV IS NULL then {{var('default_mapkey')}} else MARA.MSTAV end as CROSS_DIST_CHN_MATL_STAT_LKEY,
case when length(trim(MARA.MTPOS_MARA)) <1 then {{var('default_mapkey')}} when MARA.MTPOS_MARA IS NULL then {{var('default_mapkey')}} else MARA.MTPOS_MARA end as GENERAL_ITEM_CTGY_LKEY,
MARA.ZHNI AS HARDLINES_NEST_INCREMENT,
MARA.AESZN AS DOC_CHG_NBR,
MARA.SLED_BBD AS PROD_EXPR_DTE_TYP,
MARA.ZEIAR AS DOC_TYP,
MARA.CUOBF AS ITRL_OBJ_NBR,
MARA.ZEIVR AS DOC_VER_NBR,
case when length(trim(MARA.MSTAE)) <1 then {{var('default_mapkey')}} when MARA.MSTAE IS NULL then {{var('default_mapkey')}} else MARA.MSTAE end as CROSS_PLANT_MATL_LKEY,
MARA.DISST AS LOW_LVL_CD,
MARA.ZEINR AS DOC_NBR,
case when MARA.ZRECL = 'X' then 'Y' else 'N' end  AS PROD_RECYCLE_FLAG,
---NEW ATTRIBUTES 5/5/22
case when length(trim(MARA.PSTAT)) <1 then {{var('default_mapkey')}} when MARA.PSTAT IS NULL then {{var('default_mapkey')}} else (MARA.PSTAT) end as MTNC_STAT_TEXT,
case when length(trim(MARA.TRAGR)) <1 then {{var('default_mapkey')}} when MARA.TRAGR IS NULL then {{var('default_mapkey')}} else (MARA.TRAGR) end as TRNSPRT_GRP_LKEY,
MARA.ZMFGCELL AS DS_MFG_CELL,
MARA.PROFL AS DNGR_GOODS_TEXT,
case when length(trim(MARA.ERVOE)) <1 then {{var('default_mapkey')}} when MARA.ERVOE IS NULL then {{var('default_mapkey')}} else (MARA.ERVOE) end as VOL_UOM_ALOWD_KEY,
case when length(trim(MARA.TEMPB)) <1 then {{var('default_mapkey')}} when MARA.TEMPB IS NULL then {{var('default_mapkey')}} else (MARA.TEMPB) end as TEMRTRE_COND_LKEY
from {{source('SAPC11','MARA')}} MARA
left join {{source('SAPC11','MAKT')}} MAKT on  mara.matnr = makt.matnr and makt.spras = 'E' and MARA.MANDT=makt.MANDT
left join {{source('SAPC11','MARM')}} MARMBASE on mara.matnr = MARMBASE.Matnr and MARMBASE.Meinh = MARA.MEINS and MARA.MANDT=MARMBASE.MANDT
left join {{source('SAPC11','MARM')}}  MARMCAR on mara.matnr = MARMCAR.Matnr and MARMCAR.Meinh = 'CAR' and MARA.MANDT=MARMCAR.MANDT
left join {{source('SAPC11','MARM')}} MARMBOX on mara.matnr = MARMBOX.Matnr and MARMBOX.Meinh = 'BOX' and MARA.MANDT=MARMBOX.MANDT
WHERE MARA.MANDT={{var('sapc11mandtftr')}}