## password-alias 옵션 사용
#***** credential 생성 (-provider 옵션에 입력한 경로/파일명으로 hdfs 상에 암호화된 파일이 생성됨)
#                        <alias>                         <인증키 저장소>
hadoop credential create depadm.password.alias -provider jceks:///tmp/dep.password.jceks

#***** --password-alias 에 위에서 생성한 alias 를 사용함
sqoop export \
      -Dhadoop.security.credential.provider.path=jceks:///tmp/dep.password.jceks \
      --connect jdbc:mysql://10.178.89.142:3306/dep_dev \
      --username dep_adm \
      --password-alias dep_adm.password.alias \
      --table HDSVC_DEP_CALENDAR_T \
      --hcatalog-database DEP \
      --hcatalog-table HDCM_CALENDAR