#!/bin/bash -x
source /usr/lib/hustler/bin/qubole-bash-lib.sh
is_master=`nodeinfo is_master`
if [[ "$is_master" != "1" ]]; then
  return
fi

use_spark=`nodeinfo use_spark`
do_hbase=`nodeinfo use_hbase`
if [[ $use_spark != "1" && $do_hbase != "1" ]]; then
  return
fi

function start_zeppelin_process() {
  if [ -f /usr/lib/zeppelin/bin/zeppelin-daemon.sh ]; then
    (AWS_ACCESS_KEY_ID=; AWS_SECRET_ACCESS_KEY=; /usr/lib/zeppelin/bin/zeppelin-daemon.sh start)
  fi
}

wget localhost:8082
success=$?
if [ "$success" != "0" ]; then
  zep_pid=`jps -m | grep ZeppelinServer  | cut -d" " -f 1`
  cluster_id=`nodeinfo cluster_id`
  #take jstack of bad zeppelin process. 
  #put them in /usr/lib/zeppelin/logs/zeppelin_restart.log for debugging and then restart
  echo "------------Date: `date -u`----------------" >> /usr/lib/zeppelin/logs/zeppelin_restart.log 2>&1
  echo "zeppelin is not running fine. Zep pid is: ${zep_pid}. cluster id is: ${cluster_id}. Restarting" >> /usr/lib/zeppelin/logs/zeppelin_restart.log 2>&1
  echo "------------zeppelin jstack----------------" >> /usr/lib/zeppelin/logs/zeppelin_restart.log 2>&1
  if [ "$zep_pid" != "" ]; then
    jstack $zep_pid >> /usr/lib/zeppelin/logs/zeppelin_restart.log 2>&1 
    kill -9 ${zep_pid} 
  fi  
  zep_cid=`jps -m | grep RemoteInterpreterServer | awk '{print $1}'`
  for i in $zep_cid
  do
    echo "------------Remote JVM ${i} jstack----------------" >> /usr/lib/zeppelin/logs/zeppelin_restart.log 2>&1
    jstack $i >> /usr/lib/zeppelin/logs/zeppelin_restart.log 2>&1 
    kill -9 $i
  done
  start_zeppelin_process
fi
