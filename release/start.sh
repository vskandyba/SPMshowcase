#!/bin/bash

export HADOOP_CONF_DIR=/usr/lib/hadoop/etc/hadoop

jarFile='SPMshowcase.jar'

path_to_jar="$(pwd)/$jarFile"
path_to_settings="hdfs://quickstart.cloudera:8020/user/cloudera/SPMshowcase/settings.json"

executorEnv="spark.executorEnv.JAVA_HOME=$JAVA_HOME"
yarn_appMasterEnv="spark.yarn.appMasterEnv.JAVA_HOME=$JAVA_HOME"
sql_shuffle_partitions="spark.sql.shuffle.partitions=10"

echo 'CSV TO PARQUET'
spark-submit --master yarn --deploy-mode cluster --conf $executorEnv --conf $yarn_appMasterEnv  --class ParquetCreator $path_to_jar  $path_to_settings

echo 'CorporatePaymentsBuilder'
spark-submit --master yarn --deploy-mode cluster --conf $executorEnv --conf $yarn_appMasterEnv --conf $sql_shuffle_partitions --class CorporatePaymentsBuilder $path_to_jar  $path_to_settings

echo 'CorporateAccountBuilder'
spark-submit --master yarn --deploy-mode cluster --conf $executorEnv --conf $yarn_appMasterEnv --conf $sql_shuffle_partitions --class CorporateAccountBuilder $path_to_jar  $path_to_settings

echo 'CorporateInfoBuilder'
spark-submit --master yarn --deploy-mode cluster --conf $executorEnv --conf $yarn_appMasterEnv --conf $sql_shuffle_partitions --class CorporateInfoBuilder $path_to_jar  $path_to_settings

echo 'DBSender'
spark-submit --master yarn --deploy-mode cluster --conf $executorEnv --conf $yarn_appMasterEnv  --class DBSender $path_to_jar  $path_to_settings
