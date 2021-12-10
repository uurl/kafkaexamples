package io.treutech.plainjson

import io.treutech.{Constants, Person}
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsBuilder}
import org.apache.kafka.streams.kstream.{Consumed, Produced}

import java.time.{LocalDate, Period, ZoneId}
import java.util.Properties

//./bin/kafka-topics --create --topic ages --bootstrap-server localhost:9092 --replication-factor 1 --partitions 4

//./bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic persons --property print.key=true

object JsonStreamsProcessor {
  private final val brokers = "localhost:9092"
  private final val props = new Properties
  props.put("bootstrap.servers", brokers)
  props.put("application.id", "kafka-examples")

  def process(): Unit = {
    val streamsBuilder = new StreamsBuilder
    val personJsonStream =
      streamsBuilder.stream(Constants.getPersonsTopic, Consumed.`with`(Serdes.String, Serdes.String))
    val personStream = personJsonStream.mapValues(v => Constants.getJsonMapper.readValue(v, classOf[Person]))

    val ageStream = personStream.map((_, v) => {
        val startDateLocal = v.birthDate.toInstant.atZone(ZoneId.systemDefault).toLocalDate
        val age = Period.between(startDateLocal, LocalDate.now).getYears
        new KeyValue[String, String](v.firstName + " " + v.lastName, String.valueOf(age))
      })

    ageStream.to(Constants.getAgesTopic, Produced.`with`(Serdes.String, Serdes.String))
    val topology = streamsBuilder.build
    val streams = new KafkaStreams(topology, props)
    streams.start()
  }

  def main(args: Array[String]): Unit = process()

}