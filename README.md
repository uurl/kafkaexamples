# Kafka Examples
Kafka Examples

Download Confluent Control Center from [confluent-7.0.0](https://www.confluent.io/installation)

`Unzip confluent-7.0.0.zip`

`export CONFLUENT_HOME=<path_to>/confluent-7.0.0`

`./bin/confluent local services start`

Go to http://localhost:9021/clusters

Create 3 topics:

`./bin/kafka-topics --create --topic persons --bootstrap-server localhost:9092 --replication-factor 1 --partitions 4`

`./bin/kafka-topics --create --topic ages --bootstrap-server localhost:9092 --replication-factor 1 --partitions 4`

`./bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic persons --property print.key=true`
