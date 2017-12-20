-- 샘플파일명 : /app/dep/src/hive/AIA_HDSVC_D_DEP_MBR_INF_T.hql
-- ${INF_CD}_${TABLE_NM}.hql 형식으로 파일을 생성해야함.
-- 4-1-1 ShellScript(get_hive_data.sh) 에서 호출되어 사용되므로 HQL_PATH_NM 에 맞는 위치에 있어야함. 
-- HDFS 명령으로 바로 get 하지 않는 이유는 데이터를 가져오게 되는 대상테이블이 압축옵션이 들어간 테이블일수 있기 때문.

INSERT OVERWRITE LOCAL DIRECTORY '${vOutputLocalPath}'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
SELECT MBR_NO
     , MDN_NO
     , SEX
     , SEX_CL_CD
     , AGE
     , AGE_CL_CD
     , MBR_REG_DTM
     , MBR_END_DTM
     , MBR_STAT_CD
     , TELECOM_CL_CD
     , MKT_AGR_YN
     , OPER_DTM
  FROM HDSVC_D_DEP_MBR_INF_T
 WHERE INF_CD = '${vInfCd}'
;