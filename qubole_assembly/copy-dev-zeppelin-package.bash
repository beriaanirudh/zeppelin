#!/bin/bash -e

# This is a script to be used by developers to copy zeppelin packages to S3
# and then start spark  clusters using that package.
#
# Steps to follow:-
# 1. Say branch is ZEP-898
# 2. Create zeppelin package on that branch. Say last tag is zeppelin_qpm_0.26419471.37.
# 3. Run "qubole_assembly/copy-dev-zeppelin-package.bash ZEP-898 0.26419471.37"
# 4. Run "update cluster_configs set spark_version='dev-ZEP-898' where id = x"
#    on the rstore DB of the setup where you want to test (say devenv2).
# 5. Start the cluster.

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 <PACKAGE-NAME> <TAG>"
  echo "Example: $0 ZEP-898 0.26419471.37"
  echo -e "\tThis will copy the zeppelin-0.26419471.37.tar.gz package,"
  echo -e "\tsay, s3://dev.canopydata.com/qpm/releases/zeppelin/zeppelin-0.26419471.37.tar.gz, to"
  echo -e "\ts3://qubole-packages/archives/zeppelin/zeppelin-ZEP-898/00001-0.26419471.37.tar.gz"
  exit 1
fi

S3_PACKAGE_NAME=$1
TAG=$2
S3CMD="s3cmd -c /usr/lib/hustler/creds/s3cfg"
S3_FOLDER=s3://qubole-packages/archives/zeppelin/zeppelin-${S3_PACKAGE_NAME}
SOURCE_PACKAGE_NAME=${TAG}
SOURCE_S3_PACKAGE=s3://dev.canopydata.com/qpm/releases/zeppelin/zeppelin-${SOURCE_PACKAGE_NAME}.tar.gz

LAST_PACKAGE=$($S3CMD ls ${S3_FOLDER}/ | grep '.tar.gz$' | tail -1 | awk '{print $4;}' | awk -F/ '{print $NF;}')
LAST_PACKAGE_NUM=$(echo ${LAST_PACKAGE} | awk -F- '{print $1;}')
NEW_PACKAGE_NUM=$(expr $LAST_PACKAGE_NUM + 1)
NEW_PACKAGE_NUM=$(printf "%05d" $NEW_PACKAGE_NUM)
$S3CMD cp ${SOURCE_S3_PACKAGE} ${S3_FOLDER}/${NEW_PACKAGE_NUM}-${SOURCE_PACKAGE_NAME}.tar.gz
$S3CMD cp s3://paid-qubole/archives/zeppelin/zeppelin-1.6.0-rc/install.sh ${S3_FOLDER}/
