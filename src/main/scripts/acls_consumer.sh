topic="test_console"
if [ ! -z "$1" ]
then
topic="$1"

/usr/hdp/current/kafka-broker/bin/kafka-acls.sh \
--authorizer-properties zookeeper.connect=h11.bdiiot.com:2181,h12.bdiiot.com:2181,h13.bdiiot.com:2181 \
--remove \
--allow-principal User:bigdata \
--consumer \
--topic ${topic} \
--group \*
else

/usr/hdp/current/kafka-broker/bin/kafka-acls.sh \
--authorizer-properties zookeeper.connect=h11.bdiiot.com:2181,h12.bdiiot.com:2181,h13.bdiiot.com:2181 \
--remove \
--allow-principal User:bigdata \
--consumer \
--topic ${topic} \
--group test_console-consumer
fi