## 샘플파일명 : /home/depdev/batch_test/sprint#0-1/4-1-1.get_hive_data.sh
## HQL_PATH_NM 변수로 지정된 경로에 알맞은 HQL 파일을 미리 생성해둬야함.
## HDFS 명령으로 바로 데이터를 get 하지 않는 이유는 대상테이블이 압축옵션이 들어간 테이블일수 있기 때문.

#######################################################################
##    프로그램명: get_hive_data.sh
##    작성자    :
##    작성일자  : 2017.10.19
##    개요      :
##    적재주기  :
##    PARAMETER : $1 : 외부전송지코드, $2 : 테이블명,  $3 : 기준일자
########################################################################

#!/bin/bash

if [ $# -eq 3 ]
then
    INF_CD=$1
    TABLE_NM=$2
    BASE_DT=$3
else
    echo "usage : $0 <INF_CD> <TABLE_NM> <BASE_DT>"
    echo "ex) sh $0 AIA HDSVC_D_DEP_MBR_INF_T 20171103"
    exit 1
fi

HQL_PATH_NM="/app/dep/src/hive/${INF_CD}_${TABLE_NM}.hql"                                               # ex) /app/dep/src/hive/AIA_HDSVC_D_DEP_MBR_INF_T.hql
OUTPUT_LOCAL_PATH=`echo "/data/dep/out/${INF_CD}/${TABLE_NM}_${BASE_DT}" | tr '[:upper:]' '[:lower:]'`  # ex) /data/dep/out/aia/hdsvc_d_dep_mbr_inf_t_20171103
RESULT_PATH=`echo "/data/dep/out/${INF_CD}/${BASE_DT}" | tr '[:upper:]' '[:lower:]'`                    # ex) /data/dep/out/aia/20171103
RESULT_FILE_NM="${TABLE_NM}.dat"

LOG_PATH=`echo "/app/dep/log/${INF_CD}/${BASE_DT}" | tr '[:upper:]' '[:lower:]'`
LOG_FILE_NM=${TABLE_NM}_`date +%Y%m%d%H%M%S`.log
if [ ! -d $LOG_PATH ]
then
    mkdir -pv $LOG_PATH
fi

echo "LOG_PATH_NM : $LOG_PATH/$LOG_FILE_NM"

echo "==[Params]============================================================="  > $LOG_PATH/$LOG_FILE_NM
echo "HQL_PATH_NM : $HQL_PATH_NM"                                              >> $LOG_PATH/$LOG_FILE_NM
echo "OUTPUT_LOCAL_PATH_TEMP : $OUTPUT_LOCAL_PATH"                             >> $LOG_PATH/$LOG_FILE_NM
echo "RESULT_PATH_NM : $RESULT_PATH/$RESULT_FILE_NM"                           >> $LOG_PATH/$LOG_FILE_NM
echo "=======================================================================" >> $LOG_PATH/$LOG_FILE_NM
echo "" >> $LOG_PATH/$LOG_FILE_NM

echo "## [HIVE] INSERT OVERWRITE LOCAL DIRECTORY '$OUTPUT_LOCAL_PATH'" >> $LOG_PATH/$LOG_FILE_NM
hive -d vInfCd=$INF_CD -d vTableNm=$TABLE_NM -d vOutputLocalPath=$OUTPUT_LOCAL_PATH -f $HQL_PATH_NM &>> $LOG_PATH/$LOG_FILE_NM

echo "## Check Result File Path : $RESULT_PATH" >> $LOG_PATH/$LOG_FILE_NM
if [ ! -d $RESULT_PATH ]
then
    mkdir -pv $RESULT_PATH >> $LOG_PATH/$LOG_FILE_NM
else
    rm -rfv $RESULT_PATH   >> $LOG_PATH/$LOG_FILE_NM
    mkdir -pv $RESULT_PATH >> $LOG_PATH/$LOG_FILE_NM
fi

echo "## Merge/Move files : $OUTPUT_LOCAL_PATH/* --> $RESULT_PATH/$RESULT_FILE_NM" >> $LOG_PATH/$LOG_FILE_NM
cat $OUTPUT_LOCAL_PATH/* > $RESULT_PATH/$RESULT_FILE_NM

echo "## Remove TempDir : $OUTPUT_LOCAL_PATH" >> $LOG_PATH/$LOG_FILE_NM
rm -rfv $OUTPUT_LOCAL_PATH >> $LOG_PATH/$LOG_FILE_NM

exit 0