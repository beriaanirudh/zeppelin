# This file contains code to actually start the zeppelin server.

if [ -f /usr/lib/zeppelin/bin/zeppelin-daemon.sh ]; then
  #TODO These exports are done in zeppelin start script also now. These can
  #be removed at some later point.

  FILE="/usr/lib/qubole/cloud_info.sh" && [[ -f $FILE ]] && source $FILE
  if [[ $cloud_provider == "gce" ]]; then
    export FIRST_CLASS_NOTEBOOK_LOC=${gs_first_class_notebook_loc}
  else
    export FIRST_CLASS_NOTEBOOK_LOC=${s3_first_class_notebook_loc}
  fi

  echo "QUBOLE_BASE_URL is: ${qubole_base_url}, FIRST_CLASS_NOTEBOOK_LOC=${FIRST_CLASS_NOTEBOOK_LOC}"
  export QUBOLE_API_TOKEN=${qubole_api_token}
  export QUBOLE_BASE_URL=${qubole_base_url}
  export CLUSTER_ID=${cluster_id}
  (AWS_ACCESS_KEY_ID=; AWS_SECRET_ACCESS_KEY=;/usr/lib/zeppelin/bin/zeppelin-daemon.sh start)
fi
