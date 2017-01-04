#!/bin/bash -x

echo "------------`date -u`----------------"
find /usr/lib/zeppelin/notebook/ -empty -type f -delete
find /usr/lib/zeppelin/notebook/ -empty -type d -delete

source /usr/lib/hustler/bin/qubole-bash-lib.sh
FIRST_CLASS_NOTEBOOK_LOC=`nodeinfo s3_first_class_notebook_location`
CLUSTER_BACKUP_LOC=`nodeinfo s3_notebook_backup_location`
USE_HADOOP_CMD=`nodeinfo zeppelin_use_hadoop_cmd`

if [[ $USE_HADOOP_CMD == "true" ]]; then
   ## TO ASK: Can we run syncs in parallel in background ?
   ## Creating ${FIRST_CLASS_NOTEBOOK_LOC} and ${CLUSTER_BACKUP_LOC} if they don't exist.
   ## Default file system for sync command is hdfs. To specify local file system, use "file://"
   /usr/lib/hadoop2/bin/hadoop dfs -sync file:///usr/lib/zeppelin/notebook/* ${FIRST_CLASS_NOTEBOOK_LOC}/
   /usr/lib/hadoop2/bin/hadoop dfs -sync file:///usr/lib/zeppelin/notebook/* ${CLUSTER_BACKUP_LOC}/
else
   echo "Using s3cmd command."
   /usr/bin/s3cmd -c /usr/lib/hustler/s3cfg  --no-check-md5 sync /usr/lib/zeppelin/notebook/ ${FIRST_CLASS_NOTEBOOK_LOC}/
   /usr/bin/s3cmd -c /usr/lib/hustler/s3cfg  --no-check-md5 sync /usr/lib/zeppelin/notebook/ ${CLUSTER_BACKUP_LOC}/
fi


