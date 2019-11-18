#!/bin/bash

export KAFKA_OPTS="-Djava.security.auth.login.config=/etc/kafka/conf/kafka_jaas.conf"

topic="test_console"
if [ ! -z "$1" ]
then
topic="$1"
fi
echo ${topic}

/usr/hdp/current/kafka-broker/bin/kafka-console-consumer.sh \
--zookeeper h11.bdiiot.com:2181,h12.bdiiot.com:2181,h13.bdiiot.com:2181 \
--security-protocol PLAINTEXTSASL \
--group test_console-consumer \
--topic ${topic} \
--delete-consumer-offsets \
--from-beginning