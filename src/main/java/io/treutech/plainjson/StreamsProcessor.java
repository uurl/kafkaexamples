package io.treutech.plainjson;

import io.treutech.Constants;
import io.treutech.Person;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.util.Properties;

// $ kafka-topics --bootstrap-server localhost:9092 --create --topic ages --replication-factor 1 --partitions 4

public final class StreamsProcessor {
  private final Logger log = LogManager.getLogger(StreamsProcessor.class);

  private final String brokers;

  public StreamsProcessor(final String brokers) {
    super();
    this.brokers = brokers;
  }

  public static void main(final String[] args) {
    (new StreamsProcessor("localhost:9092")).process();
  }

  public void process() {
    final StreamsBuilder streamsBuilder = new StreamsBuilder();

    // Leemos desde el tópico de persons y deserializamos el valor como un objeto Person
    final KStream<String, String> personJsonStream = streamsBuilder.stream(
        Constants.getPersonsTopic(), Consumed.with(Serdes.String(), Serdes.String()));

    final KStream<String, Person> personStream = personJsonStream.mapValues((v -> {
      try {
        return Constants.getJsonMapper().readValue(v, Person.class);
      } catch (IOException e) {
        log.error(e.getMessage());
        return null;
      }
    }));

    // Mapeamos el flujo para calcular la edad y crear una nueva KeyValue con el nombre y la edad
    final KStream<String, String> ageStream =
        personStream.map((KeyValueMapper<String, Person, KeyValue<String, String>>) (k, v) -> {
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
