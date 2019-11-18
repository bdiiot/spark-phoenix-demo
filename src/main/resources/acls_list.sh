#!/bin/bash

/usr/hdp/current/kafka-broker/bin/kafka-acls.sh \
--authorizer-properties zookeeper.connect=h11.bdiiot.com:2181,h12.bdiiot.com:2181,h13.bdiiot.com:2181 \
--list