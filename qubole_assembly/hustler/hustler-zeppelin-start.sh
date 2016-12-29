# This file contains code to start the zeppelin server on the master.
s3_notebook_backup_loc=`nodeinfo s3_notebook_backup_location`
s3_notebook_conf_backup_loc=`nodeinfo s3_notebook_conf_backup_location`
s3_first_class_notebook_loc=`nodeinfo s3_first_class_notebook_location`
qubole_base_url=`nodeinfo qubole_base_url`
qubole_api_token=`nodeinfo qubole_cluster_api_token`
sync_enabled=`nodeinfo zeppelin_folder_sync`
is_master=`nodeinfo is_master`
use_spark=`nodeinfo use_spark`
do_hbase=`nodeinfo do_hbase`
do_presto=`nodeinfo do_presto`

FILE="/usr/lib/qubole/cloud_info.sh" && [[ -f $FILE ]] && source $FILE

if [[ $use_spark != "1" && $do_hbase != "1" && $do_presto != "1" ]]; then
  return
fi

if [[ "$is_master" == "1" ]]; then
  #zeppelin dir creation
  if [[ "$do_presto" == "1" ]]; then
    sed -i '/<name>zeppelin.interpreters<\/name>/!b;n;c<value>org.apache.zeppelin.presto.PrestoInterpreter,org.apache.zeppelin.markdown.Markdown,org.apache.zeppelin.shell.ShellInterpreter</value>' /usr/lib/zeppelin/conf/zeppelin-site.xml
  fi
  ZEP_LOG_DIR=/media/ephemeral0/logs
  zeppelin_dir="${ZEP_LOG_DIR}/zeppelin"
  zeppelin_conf_dir="/usr/lib/zeppelin/conf"
  zeppelin_log_dir="${zeppelin_dir}/logs"
  zeppelin_note_dir="${zeppelin_dir}/notebook"
  zeppelin_run_dir="${zeppelin_dir}/run"
  mkdir -p "${zeppelin_log_dir}"
  mkdir -p "${zeppelin_note_dir}/ExampleNote"

  if [[ $cloud_provider == "gce" ]]; then
    /usr/lib/hadoop2/bin/hadoop dfs -get ${gs_notebook_backup_loc}/ ${zeppelin_dir}/
    /usr/lib/hadoop2/bin/hadoop dfs -get ${gs_notebook_conf_backup_loc}/interpreter.json ${zeppelin_conf_dir}/
  else
    /usr/bin/s3cmd -c /usr/lib/hustler/s3cfg sync ${s3_notebook_conf_backup_loc}/ ${zeppelin_conf_dir}/
  fi

  if [[ $do_presto != "1" ]]; then
    cp -r /usr/lib/zeppelin/example-notebook/ExampleNote/* ${zeppelin_note_dir}/ExampleNote/
  else
    mkdir -p ${zeppelin_note_dir}/PrestoExampleNote
    cp -r /usr/lib/zeppelin/example-notebook/PrestoExampleNote/* ${zeppelin_note_dir}/PrestoExampleNote/
  fi
  # Clear interpreter.json for non-spark clusters. If a new interpreter needs to save config, this will have to change for it
  if [[ $use_spark != "1" ]]; then
    rm -f ${zeppelin_conf_dir}/interpreter.json
  fi
  mkdir -p "${zeppelin_run_dir}"
  chmod -R 777 "${zeppelin_dir}"
  source /usr/lib/zeppelin/hustler/hustler-zeppelin-start-process.sh

  if [[ $cloud_provider == "gce" ]]; then
    crontab -l | { cat; echo "*/2 * * * * /usr/lib/hadoop2/bin/hadoop sync ${zeppelin_note_dir}/ ${gs_notebook_backup_loc}/"; } | sort -u | crontab -
    crontab -l | { cat; echo "*/10 * * * * /usr/lib/hadoop2/bin/hadoop sync ${zeppelin_conf_dir}/ ${gs_notebook_conf_backup_loc}/"; } | sort -u | crontab -
    crontab -l | { cat; echo "*/2 * * * * /usr/lib/hadoop2/bin/hadoop sync ${zeppelin_note_dir}/ ${gs_first_class_notebook_loc}/"; } | sort -u | crontab -
  else
    crontab -l | { cat; echo "*/10 * * * * /usr/bin/s3cmd -c /usr/lib/hustler/s3cfg --no-check-md5 put ${zeppelin_conf_dir}/interpreter.json ${s3_notebook_conf_backup_loc}/"; } | sort -u | crontab -
    crontab -l | { cat; echo "*/2 * * * * /usr/lib/zeppelin/hustler/sync-notes-concurrent.sh"; } | sort -u | crontab -
  fi
fi
