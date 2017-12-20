-- App. 대시보드 (MART)

-- 일 방문자수/일 방문자 비율
-- 방문당 페이지뷰/방문당 체류시간
CREATE EXTERNAL TABLE IF NOT EXISTS HDSVC_D_DEP_APP_DASHBOARD_T
(
    BASE_DT            STRING COMMENT '기준일자'
  , VISIT_USER_CNT     BIGINT COMMENT '방문자수'
  , VISIT_USER_PER     DOUBLE COMMENT '방문자비율'
  , AVG_PV_CNT         DOUBLE COMMENT '방문당 페이지뷰'
  , AVG_DTIME_SEC      DOUBLE COMMENT '방문당 체류시간'
)
COMMENT 'DEP MART 일별 App 대시보드'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS ORC
LOCATION 'hdfs:///DEP/DATA/MART/SVC/STT/HDSVC_D_DEP_APP_DASHBOARD_T'
TBLPROPERTIES ('ORC.COMPRESS'='SNAPPY');

INSERT OVERWRITE TABLE HDSVC_D_DEP_APP_DASHBOARD_T PARTITION (DT='20171103')
SELECT A.BASE_DT
     , A.VISIT_USER_CNT
     , ROUND(A.VISIT_USER_CNT / B. TOT_MBR_CNT * 100, 2) AS VISIT_USER_PER
     , A.AVG_PV_CNT
     , A.AVG_DTIME_SEC
  FROM (
        SELECT BASE_DT
             , COUNT(1) AS VISIT_USER_CNT
             , AVG(PV_CNT) AS AVG_PV_CNT
             , AVG(DTIME_SEC) AS AVG_DTIME_SEC
          FROM (
                SELECT BASE_DT
                     , GRP_ID
                     , SUM(PAGE_MV_CNT) AS PV_CNT
                     , MAX(DTIME_SEC)   AS DTIME_SEC
                  FROM HDSVC_DEP_APP_LOG_DW_T
                 WHERE DT = '20171103'
                 GROUP BY
                       BASE_DT
                     , GRP_ID
               ) T
         GROUP BY BASE_DT
       ) A
  JOIN (
        SELECT '20171103' AS BASE_DT
             , COUNT(1) AS TOT_MBR_CNT
          FROM HDSVC_D_DEP_MBR_MST_T
         WHERE DT = '20171103'
       ) B
    ON A.BASE_DT = B.BASE_DT
;

-- 주 방문자수/주 방문자 비율
-- 방문당 페이지뷰/방문당 체류시간
-- 주차의 기간은 월~일 을 기준으로 샘플 작성함
CREATE EXTERNAL TABLE IF NOT EXISTS HDSVC_W_DEP_APP_DASHBOARD_T
(
    BASE_WK            STRING COMMENT '기준년주차'
  , VISIT_USER_CNT     BIGINT COMMENT '방문자수'
  , VISIT_USER_PER     DOUBLE COMMENT '방문자비율'
  , AVG_PV_CNT         DOUBLE COMMENT '방문당 페이지뷰'
  , AVG_DTIME_SEC      DOUBLE COMMENT '방문당 체류시간'
)
COMMENT 'DEP MART 주차별 App 대시보드'
PARTITIONED BY (wk STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS ORC
LOCATION 'hdfs:///DEP/DATA/MART/SVC/STT/HDSVC_W_DEP_APP_DASHBOARD_T'
TBLPROPERTIES ('ORC.COMPRESS'='SNAPPY');

-- 주 방문자수/주 방문자 비율
-- 방문당 페이지뷰/방문당 체류시간
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE HDSVC_W_DEP_APP_DASHBOARD_T PARTITION (WK)
SELECT T1.BASE_WK
     , T1.VISIT_USER_CNT
     , ROUND(T1.VISIT_USER_CNT / T2.TOT_MBR_CNT * 100, 2) AS VISIT_USER_PER -- 주 방문자비율 : 가장최근의 전체회원수 기준? 일간방문자비율의 평균?
     , T1.AVG_PV_CNT
     , T1.AVG_DTIME_SEC
     , T1.BASE_WK AS WK
  FROM (
        SELECT B1.BASE_WK
             , SUM(A1.VISIT_USER_CNT)   AS VISIT_USER_CNT
             , AVG(A1.AVG_PV_CNT)       AS AVG_PV_CNT
             , AVG(A1.AVG_DTIME_SEC)    AS AVG_DTIME_SEC
          FROM HDSVC_D_DEP_APP_DASHBOARD_T A1
          JOIN (
                SELECT A.WEEK_OF_YEAR AS BASE_WK
                     , MIN(A.BASE_DT) AS START_DT
                     , MAX(A.BASE_DT) AS END_DT
                  FROM HDSVC_DEP_CALENDAR_T A
                  JOIN (
                        SELECT WEEK_OF_YEAR
                          FROM HDSVC_DEP_CALENDAR_T
                         WHERE BASE_DT = '20171103'
                       ) B
                    ON A.WEEK_OF_YEAR = B.WEEK_OF_YEAR
                 GROUP BY A.WEEK_OF_YEAR
               ) B1
         WHERE DT BETWEEN B1.START_DT AND B1.END_DT
         GROUP BY B1.BASE_WK
       ) T1
  JOIN (
        SELECT '20171103' AS BASE_DT
             , COUNT(1)   AS TOT_MBR_CNT
          FROM HDSVC_D_DEP_MBR_MST_T
         WHERE DT = '20171103'
       ) T2
    ON 1=1
;

-- 월 방문자수/월 방문자 비율
-- 방문당 페이지뷰/방문당 체류시간
CREATE EXTERNAL TABLE IF NOT EXISTS HDSVC_M_DEP_APP_DASHBOARD_T
(
    BASE_YM            STRING COMMENT '기준일자'
  , VISIT_USER_CNT     BIGINT COMMENT '방문자수'
  , VISIT_USER_PER     DOUBLE COMMENT '방문자비율'
  , AVG_PV_CNT         DOUBLE COMMENT '방문당 페이지뷰'
  , AVG_DTIME_SEC      DOUBLE COMMENT '방문당 체류시간'
)
COMMENT 'DEP MART 월별 App 대시보드'
PARTITIONED BY (ym STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS ORC
LOCATION 'hdfs:///DEP/DATA/MART/SVC/STT/HDSVC_M_DEP_APP_DASHBOARD_T'
TBLPROPERTIES ('ORC.COMPRESS'='SNAPPY');

-- 월 방문자수/월 방문자 비율
-- 방문당 페이지뷰/방문당 체류시간
INSERT OVERWRITE TABLE HDSVC_M_DEP_APP_DASHBOARD_T PARTITION (YM='201711')
SELECT A.BASE_YM
     , A.VISIT_USER_CNT
     , ROUND(A.VISIT_USER_CNT / B.TOT_MBR_CNT * 100, 2) AS VISIT_USER_PER -- 월 방문자비율 : 가장최근의 전체회원수 기준? 일간방문자비율의 평균?
     , A.AVG_PV_CNT
     , A.AVG_DTIME_SEC
  FROM (
        SELECT SUBSTR('20171103', 1, 6) AS BASE_YM
             , SUM(VISIT_USER_CNT)      AS VISIT_USER_CNT
             , AVG(AVG_PV_CNT)          AS AVG_PV_CNT
             , AVG(AVG_DTIME_SEC)       AS AVG_DTIME_SEC
          FROM HDSVC_D_DEP_APP_DASHBOARD_T
         WHERE DT BETWEEN CONCAT(SUBSTR('20171103', 1, 6), '01') AND CONCAT(SUBSTR('20171103', 1, 6), '31')
       ) A
  JOIN (
        SELECT SUBSTR('20171103', 1, 6) AS BASE_YM
             , COUNT(1)                 AS TOT_MBR_CNT
          FROM HDSVC_D_DEP_MBR_MST_T
         WHERE DT BETWEEN CONCAT(SUBSTR('20171103', 1, 6), '01') AND CONCAT(SUBSTR('20171103', 1, 6), '31')
       ) B
    ON A.BASE_YM = B.BASE_YM
;