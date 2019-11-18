#!/bin/bash

topic="test_console"
if [ ! -z "$1" ]
then
topic="$1"
fi
echo ${topic}

/usr/hdp/current/kafka-broker/bin/kafka-topics.sh \
--zookeeper h11.bdiiot.com:2181,h12.bdiiot.com:2181,h13.bdiiot.com:2181 \
--create \
--if-not-exists \
--topic ${topic} \
--partitions 3 \
--replication-factor 3