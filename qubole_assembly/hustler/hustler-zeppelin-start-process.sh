# This file contains code to actually start the zeppelin server.

if [ -f /usr/lib/zeppelin/bin/zeppelin-daemon.sh ]; then
  #TODO These exports are done in zeppelin start script also now. These can
  #be removed at some later point.
  echo "QUBOLE_BASE_URL is: ${qubole_base_url}, S3_FIRST_CLASS_NOTEBOOK_LOC=${s3_first_class_notebook_loc}"
  export QUBOLE_API_TOKEN=${qubole_api_token}
  export QUBOLE_BASE_URL=${qubole_base_url}
  export S3_FIRST_CLASS_NOTEBOOK_LOC=${s3_first_class_notebook_loc}
  export CLUSTER_ID=${cluster_id}
  (AWS_ACCESS_KEY_ID=; AWS_SECRET_ACCESS_KEY=;/usr/lib/zeppelin/bin/zeppelin-daemon.sh start)
fi
