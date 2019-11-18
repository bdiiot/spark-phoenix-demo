#!/bin/bash

topic="test_console"
if [ ! -z "$1" ]
then
topic="$1"
fi
echo ${topic}

/usr/hdp/current/kafka-broker/bin/kafka-acls.sh \
--authorizer-properties zookeeper.connect=h11.bdiiot.com:2181,h12.bdiiot.com:2181,h13.bdiiot.com:2181 \
--add \
--allow-principal User:bigdata \
--producer \
--topic ${topic}