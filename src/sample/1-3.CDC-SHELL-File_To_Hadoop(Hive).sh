## /home/depdev/batch_test/sprint#0-1/1-3.hive_load_from_localfile.sh
## 외부시스템에서 FTP PUT 방식으로 데이터가 Interface 서버의 특정한 경로로 지정된 시간에 들어온다는 가정하에 작성.
## 전송완료 체크파일을 체크하여 파일이 존재하면 HIVE 로 LOAD 를 진행. (존재하지 않으면 5분간격으로 최대 5회 체크작업 재수행.)

#######################################################################
##    프로그램명: 1-3.hive_load_from_localfile.sh
##    작성자    :
##    작성일자  : 2017.10.18
##    개요      :
##    적재주기  :
##    PARAMETER : $1 : 소스디렉토리,  $2 : 테이블명,  $3 : 데이터기준일자
########################################################################
 
if [ $# -eq 3 ]; then
    SRC_DIR=$1
    TABLE_NM=$2
    BASE_DT=$3       
else
    echo "usage : $0 <SRC_DIR> <TABLE_NM> <BASE_DT>"
    echo "ex) sh $0 data/in/aia/20171124 HIVE_LOAD_FROM_LOCALFILE_TEST 20171124"
    exit 1
fi

#BASE_DT=`date +%Y%m%d -d "-1days"`
WORK_YMDHMS=`date +%Y%m%d%H%M%S`

SRC_FILE_PATH_NM="${SRC_DIR}/${TABLE_NM}_${BASE_DT}.dat"
CHK_FILE_PATH_NM="${SRC_DIR}/${TABLE_NM}_${BASE_DT}.chk"
HOME_PATH="/home/depdev/batch_test/sprint#0-1"
LOG_PATH=${HOME_PATH}/logs
LOG_FILE=hive_load_from_localfile_${WORK_YMDHMS}.log

echo "BASE_DT : ${BASE_DT}" > ${LOG_PATH}/${LOG_FILE}
echo "SRC_FILE_PATH_NM : ${SRC_FILE_PATH_NM}" >> ${LOG_PATH}/${LOG_FILE}
echo "CHK_FILE_PATH_NM : ${CHK_FILE_PATH_NM}" >> ${LOG_PATH}/${LOG_FILE}
echo "LOG_FILE_PATH : ${LOG_PATH}/${LOG_FILE}"

retry_cnt=0
while true
do
    if [ ! -f ${CHK_FILE_PATH_NM} ]; then
        echo "[error] checkfile not found --> ${CHK_FILE_PATH_NM}" >> ${LOG_PATH}/${LOG_FILE}
        retry_cnt=`expr $retry_cnt + 1`
    else
        break;
    fi
 
    if [ $retry_cnt -ge 5 ]; then
        echo "[error] retry_cnt : ${retry_cnt}. exit." >> ${LOG_PATH}/${LOG_FILE}
        exit 1
    fi
 
    sleep 300
done

hive -e "LOAD DATA LOCAL INPATH '${SRC_FILE_PATH_NM}' OVERWRITE INTO TABLE ${TABLE_NM} PARTITION (dt='${BASE_DT}')" &>> ${LOG_PATH}/${LOG_FILE}

exit 0