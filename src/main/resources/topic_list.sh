#!/bin/bash

/usr/hdp/current/kafka-broker/bin/kafka-topics.sh \
--zookeeper h11.bdiiot.com:2181,h12.bdiiot.com:2181,h13.bdiiot.com:2181 \
--list