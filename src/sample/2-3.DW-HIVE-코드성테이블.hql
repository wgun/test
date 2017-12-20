--  * CDC 테이블의 가장 최신일자의 LOCATION 을 바라보도록 LOCATION 변경작업을 한다.
--  * CDC 테이블은 백업용으로 파티션테이블, DW 테이블은 파티션없이 LOCATION 변경을 통해 최신데이터로 조회 가능.

ALTER TABLE HDSVC_DEP_COM_CD_T SET LOCATION 'hdfs:///DEP/DATA/DW/SVC/CDC/HDSVC_D_DEP_COM_CD_CDC_T/dt=20171017';