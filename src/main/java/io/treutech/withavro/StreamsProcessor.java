package io.treutech.withavro;

import io.treutech.Constants;
import io.treutech.Person;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;

public final class StreamsProcessor {
  private final Logger log = LogManager.getLogger(StreamsProcessor.class);

  private final String brokers;
  private final String schemaRegistryUrl;

  public StreamsProcessor(final String brokers, final String schemaRegistryUrl) {
    super();
    this.brokers = brokers;
    this.schemaRegistryUrl = schemaRegistryUrl;
  }

  public static void main(final String[] args) {
    (new StreamsProcessor("localhost:9092", "http://localhost:8081")).process();
  }

  public void process() {
    final StreamsBuilder streamsBuilder = new StreamsBuilder();
    final GenericAvroSerde avroSerde = new GenericAvroSerde();

    avroSerde.configure(
        Collections.singletonMap("schema.registry.url", schemaRegistryUrl), false);

    final KStream<String, GenericRecord> avroStream = streamsBuilder.stream(
        Constants.getPersonsAvroTopic(), Consumed.with(Serdes.String(), avroSerde));

    final KStream<String, Person> personAvroStream = avroStream.mapValues((v -> new Person(
        v.get("firstName").toString(),
        v.get("lastName").toString(),
        new Date((Long) v.get("birthDate")),
        v.get("city").toString(),
        v.get("ipAddress").toString()
    )));

    final KStream<String, String> ageStream = personAvroStream.map((k, v) -> {
        if (v == null) {
          return new KeyValue<>(k, null);
        }
        final LocalDate startDate = v.birthDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
        final int age = Period.between(startDate, LocalDate.now()).getYears();
        log.debug("Age: {}", age);
        return new KeyValue<>(v.firstName + " " + v.lastName, String.valueOf(age));
    });

    // Escribimos el flujo resultante en el tópico de ages
    ageStream.to(Constants.getAgesTopic(), Produced.with(Serdes.String(), Serdes.String()));

    final Topology topology = streamsBuilder.build();
    final Properties props = new Properties();
    props.put("bootstrap.servers", this.brokers);
    props.put("application.id", "kafka-examples");
    try (final KafkaStreams streams = new KafkaStreams(topology, props)) {
      streams.start();
    } catch (Exception e) {
      log.error("Error starting Kafka Streams: ", e);
    }
  }
}
