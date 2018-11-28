package com.treutec.plainjson;

import com.treutec.ConstantsKt;
import com.treutec.Person;

import java.io.IOException;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;

// $ kafka-topics --zookeeper localhost:2181 --create --topic ages --replication-factor 1 --partitions 4

public final class StreamsProcessor {

  private final String brokers;

  public StreamsProcessor(String brokers) {
    super();
    this.brokers = brokers;
  }

  public final void process() {
    StreamsBuilder streamsBuilder = new StreamsBuilder();

    KStream personJsonStream = streamsBuilder.stream(
        ConstantsKt.getPersonsTopic(), Consumed.with(Serdes.String(), Serdes.String()));

    KStream personStream = personJsonStream.mapValues((v -> {
      try {
        return ConstantsKt.getJsonMapper().readValue((String) v, Person.class);
      } catch (IOException e) {
        e.printStackTrace();
        return null;
      }
    }));

    KStream ageStream = personStream.map(((KeyValueMapper) (k, v) -> {
      Person person = (Person) v;
      LocalDate startDateLocal =
          person.getBirthDate().toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
      int age = Period.between(startDateLocal, LocalDate.now()).getYears();
      return new KeyValue<>(person.getFirstName() + " " + person.getLastName(), String.valueOf(age));
    }));

    ageStream.to(ConstantsKt.getAgesTopic(), Produced.with(Serdes.String(), Serdes.String()));
    Topology topology = streamsBuilder.build();
    Properties props = new Properties();
    props.put("bootstrap.servers", this.brokers);
    props.put("application.id", "kafka-examples");
    KafkaStreams streams = new KafkaStreams(topology, props);
    streams.start();
  }

  public static void main(String[] args) {
    (new StreamsProcessor("localhost:9092")).process();
  }
}

