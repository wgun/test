USE DEP;

-- DEP 공통 달력
CREATE EXTERNAL TABLE IF NOT EXISTS HDCM_CALENDAR
(
    STD_DT           STRING COMMENT '기준일자'
  , YYYY             STRING COMMENT '기준년'
  , MM               STRING COMMENT '기준월'
  , DD               STRING COMMENT '기준일'
  , WEEK_OF_YEAR     STRING COMMENT '주차코드'
  , DAY_OF_WEEK      STRING COMMENT '요일코드'
  , DAY_OF_WEEK_S    STRING COMMENT '요일명'
  , DAY_OF_YEAR      STRING COMMENT '연간일자수'
  , WEEK_END_YN      STRING COMMENT '주말여부YN'
)
COMMENT 'DEP 공통 달력 테스트'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
LOCATION 'hdfs:///DEP/DATA/MART/SVC/STT/HDCM_CALENDAR';


set hivevar:start_date=2017-01-01;
set hivevar:days=1825;
-- 생성할 달력의 시작일자와 생성할 날짜수로 사용될 변수 설정
-- 2017-01-01 ~ 2021-12-31

INSERT OVERWRITE TABLE HDCM_CALENDAR
SELECT STD_DT
     , YYYY
     , MM
     , DD
       -- 기준년과 바로 CONCAT 을 하게되면 년도가 바뀌면서 발생되는 문제가 있어 아래와 같이 년도를 변환(+- 처리) 후 적용함.
       -- (WeekOfYear Function : A week is considered to start on a Monday and week 1 is the first week with > 3 days.)
     , CONCAT(CASE WHEN DAY_OF_YEAR <= 3   AND WEEK_OF_YEAR >= 52 THEN CAST(YYYY - 1 AS BIGINT)
                   WHEN DAY_OF_YEAR >= 363 AND WEEK_OF_YEAR  = 1  THEN CAST(YYYY + 1 AS BIGINT)
                   ELSE YYYY END
            , LPAD(WEEK_OF_YEAR, 2, '0')) AS WEEK_OF_YEAR
     , DAY_OF_WEEK
     , DAY_OF_WEEK_S
     , DAY_OF_YEAR
     , WEEK_END_YN
  FROM (
        SELECT REGEXP_REPLACE(STD_DT_FMT, '-', '') AS STD_DT
             , YEAR(STD_DT_FMT)                    AS YYYY
             , LPAD(MONTH(STD_DT_FMT), 2, '0')     AS MM
             , LPAD(DAY(STD_DT_FMT), 2 , '0')      AS DD
             , WeekOfYear(STD_DT_FMT)              AS WEEK_OF_YEAR
             , DATE_FORMAT(STD_DT_FMT, 'u')        AS DAY_OF_WEEK
             , CASE DATE_FORMAT(STD_DT_FMT, 'EEE') WHEN 'Mon' THEN '월'
                                                   WHEN 'Tue' THEN '화'
                                                   WHEN 'Wed' THEN '수'
                                                   WHEN 'Thu' THEN '목'
                                                   WHEN 'Fri' THEN '금'
                                                   WHEN 'Sat' THEN '토'
                                                   WHEN 'Sun' THEN '일'
                END                                AS DAY_OF_WEEK_S
             , DATE_FORMAT(STD_DT_FMT, 'D')        AS DAY_OF_YEAR
             , CASE WHEN DATE_FORMAT(STD_DT_FMT, 'u') BETWEEN 6 AND 7 THEN 'Y'
                    ELSE 'N'
                END                                AS WEEK_END_YN
          FROM (
                SELECT DATE_ADD('${start_date}', A.POS) AS STD_DT_FMT
                  FROM (SELECT POSEXPLODE(SPLIT(REPEAT(',', ${days}), ','))) A
               ) T
       ) TT
  SORT BY STD_DT
;


-- 기준일자로 주차코드에 해당하는 시작일자 종료일자 구하기 예시
SELECT A.WEEK_OF_YEAR AS STD_WK
     , MIN(A.STD_DT)  AS START_DT
     , MAX(A.STD_DT)  AS END_DT
  FROM HDCM_CALENDAR A
  JOIN (
        SELECT WEEK_OF_YEAR
          FROM HDCM_CALENDAR
         WHERE STD_DT = '20171114'
       ) B
    ON A.WEEK_OF_YEAR = B.WEEK_OF_YEAR
 GROUP BY A.WEEK_OF_YEAR
;


-- MariaDB DDL
CREATE TABLE OCM_CALENDAR (
    STD_DT        VARCHAR(8) NOT NULL COMMENT '기준일자'
  , YYYY          VARCHAR(4) NOT NULL COMMENT '기준년'
  , MM            VARCHAR(2) NOT NULL COMMENT '기준월'
  , DD            VARCHAR(2) NOT NULL COMMENT '기준일'
  , WEEK_OF_YEAR  VARCHAR(6) NOT NULL COMMENT '주차코드'
  , DAY_OF_WEEK   VARCHAR(1) NOT NULL COMMENT '요일코드'
  , DAY_OF_WEEK_S VARCHAR(1) NOT NULL COMMENT '요일명'
  , DAY_OF_YEAR   VARCHAR(3) NOT NULL COMMENT '연간일자수'
  , WEEK_END_YN   VARCHAR(1) NOT NULL COMMENT '주말여부YN'
)
COMMENT='DEP 공통 달력'
COLLATE='utf8_general_ci'
ENGINE=InnoDB
;

-- Hive -> MariaDB Sqoop Export
sqoop export \
      -Dhadoop.security.credential.provider.path=jceks:///tmp/dep.password.jceks \
      --connect jdbc:mysql://10.178.89.142/dep_dev \
      --username usr_anl \
      --password-alias usr_anl.password.alias \
      --table OCM_CALENDAR \
      --hcatalog-database DEP \
      --hcatalog-table HDCM_CALENDAR