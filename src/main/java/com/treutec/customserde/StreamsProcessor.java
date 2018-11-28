package com.treutec.customserde;

import com.treutec.Constants;
import com.treutec.Person;
import com.treutec.custom.PersonSerde;

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
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public final class StreamsProcessor {
  private final Logger logger;
  private final String brokers;

  public StreamsProcessor(String brokers) {
    super();
    this.brokers = brokers;
    this.logger = LogManager.getLogger(this.getClass());
  }

  public final void process() {
    StreamsBuilder streamsBuilder = new StreamsBuilder();
    PersonSerde personSerde = new PersonSerde();
    KStream personStream = streamsBuilder.stream(
        Constants.getPersonsTopic(), Consumed.with(Serdes.String(), personSerde));

    KStream ageStream = personStream.map((KeyValueMapper) (k, v) -> {
      Person person = (Person) v;

      LocalDate birthDateLocal =
          person.getBirthDate().toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
      int age = Period.between(birthDateLocal, LocalDate.now()).getYears();
      logger.debug("Age: " + age);
      return new KeyValue<>(person.getFirstName() + ' ' + person.getLastName(), String.valueOf(age));
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
    (new StreamsProcessor("localhost:9092")).process();
  }
}
