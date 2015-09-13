# This file contains code to start the zeppelin server on the master.

if [[ $use_spark != "1" && $do_hbase != "1" ]]; then
  return
fi

if [[ "$is_master" == "1" ]]; then
  #zeppelin dir creation
  zeppelin_dir="${LOG_DIR}/zeppelin"
  zeppelin_conf_dir="/usr/lib/zeppelin/conf"
  zeppelin_log_dir="${zeppelin_dir}/logs"
  zeppelin_note_dir="${zeppelin_dir}/notebook"
  zeppelin_run_dir="${zeppelin_dir}/run"
  mkdir -p "${zeppelin_log_dir}"
  mkdir -p "${zeppelin_note_dir}/ExampleNote"
  /usr/bin/s3cmd -c /usr/lib/hustler/s3cfg sync ${s3_notebook_backup_loc}/ ${zeppelin_note_dir}/
  /usr/bin/s3cmd -c /usr/lib/hustler/s3cfg sync ${s3_notebook_conf_backup_loc}/ ${zeppelin_conf_dir}/
  cp -r /usr/lib/zeppelin/example-notebook/ExampleNote/* ${zeppelin_note_dir}/ExampleNote/
  mkdir -p "${zeppelin_run_dir}"
  chmod -R 777 "${zeppelin_dir}"
  source /usr/lib/zeppelin/hustler/hustler-zeppelin-start-process.sh
  crontab -l | { cat; echo "*/2 * * * * /usr/bin/s3cmd -c /usr/lib/hustler/s3cfg --delete-removed --no-check-md5 sync ${zeppelin_note_dir}/ ${s3_notebook_backup_loc}/"; } | sort -u | crontab -
  crontab -l | { cat; echo "*/10 * * * * /usr/bin/s3cmd -c /usr/lib/hustler/s3cfg --no-check-md5 put ${zeppelin_conf_dir}/interpreter.json ${s3_notebook_conf_backup_loc}/"; } | sort -u | crontab -
  crontab -l | { cat; echo "*/2 * * * * /usr/bin/s3cmd -c /usr/lib/hustler/s3cfg --no-check-md5 sync ${zeppelin_note_dir}/ ${s3_first_class_notebook_loc}/"; } | sort -u | crontab -
fi
