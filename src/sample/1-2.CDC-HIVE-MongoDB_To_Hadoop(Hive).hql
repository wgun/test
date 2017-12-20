----[MongoDB]--------------------------------------------------------------------------------------------------------------------------------
-- * 테스트용 몽고디비 설치(skcc-dep-db1-demo) 후 데이터베이스 / 테이블 생성 / 테스트데이터 Insert
# mongo

use test_db

-- SampleTable Create & SampleData Insert
db.dep_app_log_test.insert({logDtm: "20171020105451000", sessionId: "BF8452E2C7D38220", deviceID: "355715565309247", currPageId: "0x01"})

-- DataList
db.dep_app_log_test.findOne()
{
        "_id" : ObjectId("59e95f1c9ebeb2ac687bc392"),
        "logDtm" : "20171020105451000",
        "sessionId" : "BF8452E2C7D38220",
        "deviceId" : "355715565309247",
        "currPageId" : "0x01"
}



----[HIVE]-----------------------------------------------------------------------------------------------------------------------------------
-- * MongoDB 를 타겟으로 갖는 Hive 테이블을 생성/조회 를 위해 jar 를 추가.
-- * 아래 순서가 변경되면 정상적으로 동작하지않으니 주의.
add jar hdfs:///DEP/LIB/mongo-java-driver-3.5.0.jar;
add jar hdfs:///DEP/LIB/mongo-hadoop-core-2.0.2.jar;
add jar hdfs:///DEP/LIB/mongo-hadoop-hive-2.0.2.jar;

-- Hive 테이블 생성
-- * 생성된 테이블은 MongoDB 에 연결되어 있는 테이블이기 때문에 이 테이블을 직접적으로 이용하지 않고
--   별도의 CDC 테이블에 필요한 데이터만 필터링 후 적재하여 작업을 수행해야한다.
CREATE EXTERNAL TABLE IF NOT EXISTS DEP_APP_LOG_MONGO_TEST
( 
    ID              STRING COMMENT 'SEQ_ID'
  , LOG_DTM         STRING COMMENT '로그일시'
  , SESSION_ID      STRING COMMENT '세션ID'
  , DEVICE_ID       STRING COMMENT '디바이스ID'
  , MBR_NO          STRING COMMENT '회원번호'
  , CURR_PAGE_ID    STRING COMMENT '현재페이지'
  , INPATH_CD       STRING COMMENT '유입경로코드'
)
STORED BY 'com.mongodb.hadoop.hive.MongoStorageHandler'
WITH SERDEPROPERTIES('mongo.columns.mapping'='{"ID":"_id"
                                             , "LOG_DTM":"logDtm"
                                             , "SESSION_ID":"sessionId"
                                             , "DEVICE_ID":"deviceId"
                                             , "MBR_NO":"mbrNo"
                                             , "CURR_PAGE_ID":"currPageId"
                                             , "INPATH_CD":"inpathCd"}')
TBLPROPERTIES('mongo.uri'='mongodb://skcc-dep-db1-demo:27017/test_db.dep_app_log_test');



-- 적용이 잘 되었는지 조회 확인
hive> select * from DEP_APP_LOG_MONGO_TEST;
OK
59e95f1c9ebeb2ac687bc392        20171020105451000       BF8452E2C7D38220        355715565309247 NULL    0x01    NULL
Time taken: 0.14 seconds, Fetched: 1 row(s)
