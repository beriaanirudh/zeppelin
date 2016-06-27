# The script to sync notes to S3 (sync-notes.sh) is being called by the 2 min
# crontab, and also by zeppelin server while commiting or restoring a notebook. To
# protect this sync against any concurrency errors, flock is used to obtain
# lock on a file before executing the script. If lock is already acquired, this
# script exits with exit status = 1.

ZEP_LOG_DIR=/media/ephemeral0/logs/zeppelin/logs/

filename=${ZEP_LOG_DIR}/s3_sync.lock

flock -n $filename /usr/lib/zeppelin/hustler/sync-notes.sh >> ${ZEP_LOG_DIR}/sync.log 2>&1