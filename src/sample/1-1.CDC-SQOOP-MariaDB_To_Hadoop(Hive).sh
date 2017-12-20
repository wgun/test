## mapper 개수를 늘리고 싶은 경우 --split-by <column> 옵션 사용
## 조건 필터링 없이 전체데이터의 특정 컬럼만 import 하는 경우 --columns <column,column,...> 옵션 사용
 
# a. 전체데이터
sqoop import \
--connect jdbc:mysql://10.178.89.142:3306/dep_dev \
--username 'dep_adm' \
--password 'dep_adm' \
--fields-terminated-by "\001" \
--table com_cd_test \
--hive-import \
--hive-table HDSVC_D_DEP_COM_CD_CDC_T \
--hive-partition-key 'dt' \
--hive-partition-value '20171017' \
--delete-target-dir \
--target-dir /DEP/DATA/DW/SVC/CDC/HDSVC_D_DEP_COM_CD_CDC_T/dt=20171017 \
--m 1

# b. 조건 필터링이 필요한 데이터
sqoop import \
--connect jdbc:mysql://10.178.89.142:3306/dep_dev \
--username 'dep_adm' \
--password 'dep_adm' \
--fields-terminated-by "\001" \
--query "SELECT MBR_NO, MDN_NO, SEX, AGE, MBR_REG_DTM, MBR_END_DTM, MBR_STAT_CD, TELECOM_CL_CD, MKT_AGR_YN, OPER_DTM FROM dep_mbr_test WHERE \$CONDITIONS AND DATE_FORMAT(OPER_DTM, '%Y%m%d') = '20171103'" \
--hive-import \
--hive-table HDSVC_D_DEP_MBR_CDC_T \
--hive-partition-key 'dt' \
--hive-partition-value '20171103' \
--delete-target-dir \
--target-dir /DEP/DATA/DW/SVC/CDC/HDSVC_D_DEP_MBR_CDC_T/dt=20171103 \
--m 1


# c. ORC-SNAPPY압축방식을 적용하여 Import 하는 경우
## --hive-overwrite, --delete-target-dir 옵션을 사용할 수 없기 때문에
##   재작업시에는 수동으로 타겟테이블의 LOCATION 을 비워주는 선행작업이 필요하며, 작업하지 않으면 추출된 파일이 append 된다.
## 타겟테이블(파티션)의 LOCATION으로 IMPORT되며 다른 LOCATION으로 변경하기 위한 --target-dir 옵션을 사용할 수 없다.
sqoop import \
--connect jdbc:mysql://10.178.89.142:3306/dep_dev \
--username 'dep_adm' \
--password 'dep_adm' \
--table customer_test \
--fields-terminated-by "\001" \
--hcatalog-database default \
--hcatalog-table CUSTOMER_TEST_ORC \
--hcatalog-storage-stanza 'stored as orcfile tblproperties ("orc.compress"="SNAPPY")' \
--hcatalog-partition-keys dt \
--hcatalog-partition-values 20171016 \
--m 1
