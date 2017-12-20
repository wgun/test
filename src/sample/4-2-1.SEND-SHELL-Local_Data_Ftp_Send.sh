## 샘플파일명 : /home/depdev/batch_test/sprint#0-1/4-2-1.dep_sftp_send.sh
## SFTP는 쉘스크립트상에서 실행시 패스워드를 자동으로 넣어주는 기능이 없기때문에
## 전송지에서 공개키를 생성해서 INTERFACE 서버에 전송해주어야 패스워드 없이 접속이 가능함.
## 전송지 : ssh-keygen -t dsa -P '' -f ~/.ssh/id_dsa
##          cat ~/.ssh/id_dsa.pub >> ~/.ssh/authorized_keys
##          ~/.ssh/id_dsa 파일을 원격지의 ~/.ssh/id_dsa 파일에 이어써준다.(없으면 생성)

#######################################################################
##    프로그램명: 4-2-1.dep_sftp_send.sh
##    작성자    :
##    작성일자  : 2017.11.14
##    개요      :
##    적재주기  :
##    PARAMETER : $1 : 설정파일, $2 : 소스디렉토리,  $3 : 타겟디렉토리,  $4 : 타겟파일
########################################################################

#!/bin/bash

if [ $# -eq 4 ]; then
    INI_FILE=$1
    SRC_DIR=$2
    TARGET_DIR=$3
    TARGET_FILE=$4
else
    echo "usage : $0 <INI_FILE> <SRC_DIR> <TARGET_DIR> <TARGET_FILE>"
    echo "ex) sh $0 ini/dep_ha3_demo.ini data/out/aia/TEST_DATA /home/depdev TEST_DATA" 
    exit 1
fi

WORK_DT=`date +%Y%m%d`

echo "INI_FILE : ${INI_FILE}"
echo "SRC_DIR : ${SRC_DIR}"
echo "TARGET_DIR : ${TARGET_DIR}"
echo "TARGET_FILE : ${TARGET_FILE}"

HOME_PATH=/home/depdev/batch_test/sprint#0-1
LOG_PATH=${HOME_PATH}/logs
LOG_FILE=dep_sftp_send_${WORK_DT}.log

HOST_IP=`grep -v '#' ${INI_FILE} | awk -F',' '{print $1}'`
USER_ID=`grep -v '#' ${INI_FILE} | awk -F',' '{print $2}'`

echo "["`date "+%Y-%m-%d %H:%M:%S"`"] SFTP Data Send Start." >> ${LOG_PATH}/${LOG_FILE}

sftp ${USER_ID}@${HOST_IP} << ! &>> ${LOG_PATH}/${LOG_FILE}
lcd ${SRC_DIR}
cd ${TARGET_DIR}
put ${TARGET_FILE}
ls -l
quit
!

echo "["`date "+%Y-%m-%d %H:%M:%S"`"] SFTP Data Send End." >> ${LOG_PATH}/${LOG_FILE}

exit 0