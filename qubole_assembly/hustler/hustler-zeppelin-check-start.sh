# This file contains code to check if the zeppelin server started fine.
# If it did not, then it is restarted.

if [[ "$is_master" != "1" ]]; then
  return
fi

if [[ $use_spark != "1" && $do_hbase != "1" ]]; then
  return
fi

for i in 1 2 3 4 5
do
  wget localhost:8082
  success=$?
  if [ "$success" != "0" ]; then
    zep_pid=`jps -m | grep ZeppelinServer  | cut -d" " -f 1`
    echo "zeppelin is not running fine. Zep pid is: ${zep_pid}. Restarting"
    if [ "$zep_pid" != "" ]; then
      kill -9 ${zep_pid} 
    fi
    zep_cid=`jps -m | grep RemoteInterpreterServer | awk '{print $1}'`
    for i in $zep_cid
    do
      echo "Killing: $i"
      kill -9 $i
    done
    source /usr/lib/zeppelin/hustler/hustler-zeppelin-start-process.sh
  else
    echo "zeppelin is running fine..."
    break
  fi
  sleep 20
done

if [ -f /usr/lib/zeppelin/hustler/zeppelin_restart.sh ]; then
  chmod 755 /usr/lib/zeppelin/hustler/zeppelin_restart.sh
  crontab -l | { cat; echo "*/10 * * * * /usr/lib/zeppelin/hustler/zeppelin_restart.sh > /dev/null 2>&1"; } | sort -u | crontab -
fi
