package io.treutech.withavro;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.treutech.Constants;
import io.treutech.Person;

import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;


public final class StreamsProcessor {
  private final String brokers;
  private final String schemaRegistryUrl;

  public StreamsProcessor(String brokers, String schemaRegistryUrl) {
    super();
    this.brokers = brokers;
    this.schemaRegistryUrl = schemaRegistryUrl;
  }

  public void process() {
    StreamsBuilder streamsBuilder = new StreamsBuilder();
    GenericAvroSerde avroSerde = new GenericAvroSerde();

    avroSerde.configure(
        Collections.singletonMap("schema.registry.url", schemaRegistryUrl), false);

    KStream<String, GenericRecord> avroStream = streamsBuilder.stream(
        Constants.getPersonsAvroTopic(), Consumed.with(Serdes.String(), avroSerde));

    KStream<String, Person> personAvroStream = avroStream.mapValues((v -> new Person(
        v.get("firstName").toString(),
        v.get("lastName").toString(),
        new Date((Long) v.get("birthDate")),
        v.get("city").toString(),
        v.get("ipAddress").toString()
    )));

    KStream<String, String> ageStream = personAvroStream.map((k, v) -> {
      LocalDate birthDateLocal = v.birthDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
      int age = Period.between(birthDateLocal, LocalDate.now()).getYears();
      return new KeyValue<>(v.firstName + ' ' + v.lastName, String.valueOf(age));
    });

    ageStream.to(Constants.getAgesTopic(), Produced.with(Serdes.String(), Serdes.String()));
    Topology topology = streamsBuilder.build();
    Properties props = new Properties();
    props.put("bootstrap.servers", this.brokers);
    props.put("application.id", "kafka-examples");
    KafkaStreams streams = new KafkaStreams(topology, props);
    streams.start();
  }

  public static void main(String[] args) {
    (new StreamsProcessor("localhost:9092", "http://localhost:8081")).process();
  }
}