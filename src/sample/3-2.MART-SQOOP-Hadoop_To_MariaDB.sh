## Mart 테이블의 파일저장방식이 ORC-SNAPPY 로 되어있기 때문에 hcatalog 옵션들을 사용해야함
## 작업을 재수행 하는 경우를 위해 중복되지 않게 해당 기간의 데이터를 삭제 후 EXPORT.

# 1. 회원현황 (일)
sqoop eval \
      --connect jdbc:mysql://10.178.89.142/dep_dev \
      --username dep_adm \
      --password dep_adm \
      --query "DELETE FROM HDSVC_D_DEP_MBR_INFO_T WHERE BASE_DT = '20171103'"

sqoop export                                      \
      --connect jdbc:mysql://10.178.89.142/dep_dev \
      --username dep_adm                           \
      --password dep_adm                           \
      --table HDSVC_D_DEP_MBR_INFO_T               \
      --hcatalog-database default                  \
      --hcatalog-table HDSVC_D_DEP_MBR_INFO_T      \
      --hcatalog-partition-keys dt                 \
      --hcatalog-partition-values 20171103


# 2-1. App. Dashboard (일)
sqoop eval \
      --connect jdbc:mysql://10.178.89.142/dep_dev \
      --username dep_adm \
      --password dep_adm \
      --query "DELETE FROM HDSVC_D_DEP_APP_DASHBOARD_T WHERE BASE_DT = '20171103'"

sqoop export \
      --connect jdbc:mysql://10.178.89.142/dep_dev \
      --username dep_adm \
      --password dep_adm \
      --table HDSVC_D_DEP_APP_DASHBOARD_T \
      --hcatalog-database default \
      --hcatalog-table HDSVC_D_DEP_APP_DASHBOARD_T \
      --hcatalog-partition-keys dt \
      --hcatalog-partition-values 20171103


# 2-2. App. Dashboard (주)
# 주 통계의 경우 주차코드를 사용하기때문에 hive 를 통해 주차코드를 가져오는 작업이 필요함
WEEK_OF_YEAR=`hive -e "SELECT WEEK_OF_YEAR FROM HDSVC_DEP_CALENDAR_T WHERE BASE_DT = '20171103'"`

sqoop eval \
      --connect jdbc:mysql://10.178.89.142/dep_dev \
      --username dep_adm \
      --password dep_adm \
      --query "DELETE FROM HDSVC_W_DEP_APP_DASHBOARD_T WHERE BASE_WK = '${WEEK_OF_YEAR}'"

sqoop export \
      --connect jdbc:mysql://10.178.89.142/dep_dev \
      --username dep_adm \
      --password dep_adm \
      --table HDSVC_W_DEP_APP_DASHBOARD_T \
      --hcatalog-database default \
      --hcatalog-table HDSVC_W_DEP_APP_DASHBOARD_T \
      --hcatalog-partition-keys wk \
      --hcatalog-partition-values ${WEEK_OF_YEAR}


# 2-3. App. Dashboard (월)
sqoop eval \
      --connect jdbc:mysql://10.178.89.142/dep_dev \
      --username dep_adm \
      --password dep_adm \
      --query "DELETE FROM HDSVC_M_DEP_APP_DASHBOARD_T WHERE BASE_YM = '201711'"

sqoop export \
      --connect jdbc:mysql://10.178.89.142/dep_dev \
      --username dep_adm \
      --password dep_adm \
      --table HDSVC_M_DEP_APP_DASHBOARD_T \
      --update-key BASE_YM \
      --update-mode allowinsert \
      --hcatalog-database default \
      --hcatalog-table HDSVC_M_DEP_APP_DASHBOARD_T \
      --hcatalog-partition-keys ym \
      --hcatalog-partition-values 201711