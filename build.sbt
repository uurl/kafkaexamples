name := "kafkaexamples"

version := "0.1"

scalaVersion := "2.13.7"

resolvers += "maven" at "https://packages.confluent.io/maven/"

libraryDependencies ++= List(
  "com.github.javafaker" % "javafaker" % "1.0.2",
  "org.slf4j" % "slf4j-api" % "1.7.32",
  "org.slf4j" % "slf4j-log4j12" % "1.7.32",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.0",
  "io.confluent" % "kafka-avro-serializer" % "5.0.0",
  "io.confluent" % "kafka-streams-avro-serde" % "5.0.0",
  "org.apache.kafka" % "kafka-clients" % "3.0.0",
  "org.apache.kafka" % "kafka-streams" % "3.0.0"
)
