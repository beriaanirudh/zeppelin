#!/bin/bash -x

echo "------------`date -u`----------------"
find /usr/lib/zeppelin/notebook/ -empty -type f -delete
find /usr/lib/zeppelin/notebook/ -empty -type d -delete

source /usr/lib/hustler/bin/qubole-bash-lib.sh
FIRST_CLASS_NOTEBOOK_LOC=`nodeinfo s3_first_class_notebook_location`
CLUSTER_BACKUP_LOC=`nodeinfo s3_notebook_backup_location`

/usr/bin/s3cmd -c /usr/lib/hustler/s3cfg  --no-check-md5 sync /usr/lib/zeppelin/notebook/ ${FIRST_CLASS_NOTEBOOK_LOC}/
/usr/bin/s3cmd -c /usr/lib/hustler/s3cfg  --no-check-md5 sync /usr/lib/zeppelin/notebook/ ${CLUSTER_BACKUP_LOC}/

