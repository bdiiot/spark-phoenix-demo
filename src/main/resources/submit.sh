#!/bin/bash

hadoop fs -rm -r -f /tmp/{checkpoint_realtime,mysql_to_ods}

export SPARK_MAJOR_VERSION=2

spark-submit \
--master yarn \
--deploy-mode client \
--conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=/etc/kafka/conf/kafka_jaas.conf" \
--class com.bdiiot.spark.phoenix.main.SparkPhoenixMain \
spark-phoenix-demo-1.0-SNAPSHOT.jar