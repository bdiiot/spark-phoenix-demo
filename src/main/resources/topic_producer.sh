#!/bin/bash

topic="test_console"
if [ ! -z "$1" ]
then
topic="$1"
fi
echo ${topic}

/usr/hdp/current/kafka-broker/bin/kafka-console-producer.sh \
--broker-list h11.bdiiot.com:6667,h12.bdiiot.com:6667,h13.bdiiot.com:6667 \
--security-protocol PLAINTEXTSASL \
--topic ${topic}