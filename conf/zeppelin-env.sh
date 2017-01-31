#!/bin/bash

# export JAVA_HOME=
# export HADOOP_HOME=            # Required except for setting with Hive Server

export MASTER=yarn-client
# spark master
# export SPARK_HOME=             # spark home
export HADOOP_CONF_DIR=/usr/lib/hadoop2/etc/hadoop/
export SPARK_YARN_USER_ENV="CLASSPATH=/usr/lib/hadoop2/etc/hadoop/"

# export ZEPPELIN_CONF_DIR       # Alternate zeppelin conf dir. Default is ${HBASE_HOME}/conf.
# export ZEPPELIN_LOG_DIR        # Where log files are stored.  PWD by default.
# export ZEPPELIN_PID_DIR        # The pid files are stored. /tmp by default.
# export ZEPPELIN_NOTEBOOK_DIR   # Where notebook saved
# export ZEPPELIN_IDENT_STRING   # A string representing this instance of zeppelin. $USER by default
# export ZEPPELIN_NICENESS       # The scheduling priority for daemons. Defaults to 0.
# export ZEPPELIN_JAVA_OPTS      # Additional jvm option
export PYTHONPATH="$SPARK_HOME/python/:$PYTHONPATH"
export PYTHONPATH="$SPARK_HOME/python/lib/py4j-0.8.2.1-src.zip:$PYTHONPATH"
# export PYSPARK_PYTHON          # path to the python command. must be the same path on the driver(Zeppelin) and all workers.
# export PYTHONPATH              # extra PYTHONPATH.
if [[ -d "/usr/lib/spark" ]]
then
export SPARK_HOME=/usr/lib/spark
fi

source /usr/lib/hustler/bin/qubole-bash-lib.sh
ZEPPELIN_USE_RECOMMENDED_JAVA_VERSION=`nodeinfo zeppelin_use_recommended_java_version`
if [[ "true" == "${ZEPPELIN_USE_RECOMMENDED_JAVA_VERSION}" ]]; then
  export JAVA_HOME=/usr/java/jdk1.7.0_67
fi
