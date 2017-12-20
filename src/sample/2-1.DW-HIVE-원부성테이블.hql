-- D-2 일자 HDMB_HEALTH_MESURE 테이블 데이터와 D-1 일자 HDMB_HEALTH_MESURE_CDC 파티션 데이터 MARGE
USE DEP;

dfs -mkdir -p /DEP/DATA/DW/SVC/BAK/HDMB_MBER/20171119;
dfs -cp -f /DEP/DATA/DW/SVC/DW/HDMB_MBER/* /DEP/DATA/DW/SVC/BAK/HDMB_MBER/20171119/;  -- */

INSERT OVERWRITE TABLE HDMB_MBER
SELECT IF(A.MBR_ID IS NOT NULL, A.MBR_ID      , B.MBR_ID      ) AS MBR_ID
     , IF(A.MBR_ID IS NOT NULL, A.MBR_NM      , B.MBR_NM      ) AS MBR_NM
     , IF(A.MBR_ID IS NOT NULL, A.BRTDT       , B.BRTDT       ) AS BRTDT
     , IF(A.MBR_ID IS NOT NULL, A.SEX_CD      , B.SEX_CD      ) AS SEX_CD
     , IF(A.MBR_ID IS NOT NULL, A.NAT_CD      , B.NAT_CD      ) AS NAT_CD
     , IF(A.MBR_ID IS NOT NULL, A.SELF_AUTH_ID, B.SELF_AUTH_ID) AS SELF_AUTH_ID
     , IF(A.MBR_ID IS NOT NULL, A.MBR_SCRB_DTM, B.MBR_SCRB_DTM) AS MBR_SCRB_DTM
     , IF(A.MBR_ID IS NOT NULL, A.FST_RGST_ID , B.FST_RGST_ID ) AS FST_RGST_ID
     , IF(A.MBR_ID IS NOT NULL, A.FST_RGST_DTM, B.FST_RGST_DTM) AS FST_RGST_DTM
     , IF(A.MBR_ID IS NOT NULL, A.LST_CHGT_ID , B.LST_CHGT_ID ) AS LST_CHGT_ID
     , IF(A.MBR_ID IS NOT NULL, A.LST_CHG_DTM , B.LST_CHG_DTM ) AS LST_CHG_DTM
     , IF(A.MBR_ID IS NOT NULL, A.OPER_DTM    , B.OPER_DTM    ) AS OPER_DTM
  FROM (
        SELECT COALESCE(MBR_ID              ,NULL) AS MBR_ID
             , COALESCE(MBR_NM              , '#') AS MBR_NM
             , COALESCE(BRTDT               , '#') AS BRTDT
             , COALESCE(SEX_CD              , '#') AS SEX_CD
             , COALESCE(NAT_CD              , '#') AS NAT_CD
             , COALESCE(SELF_AUTH_ID        ,  0 ) AS SELF_AUTH_ID
             , COALESCE(MBR_SCRB_DTM        , '#') AS MBR_SCRB_DTM
             , COALESCE(FST_RGST_ID         , '#') AS FST_RGST_ID
             , COALESCE(FST_RGST_DTM        , '#') AS FST_RGST_DTM
             , COALESCE(LST_CHGT_ID         , '#') AS LST_CHGT_ID
             , COALESCE(LST_CHG_DTM         , '#') AS LST_CHG_DTM
             , from_unixtime(unix_timestamp())     AS OPER_DTM
          FROM HDMB_MBER_CDC
         WHERE DT = '20171120'
       ) A
  FULL OUTER JOIN HDMB_MBER B
    ON A.MBR_ID = B.MBR_ID
;