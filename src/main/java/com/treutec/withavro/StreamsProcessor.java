package com.treutec.withavro;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import com.treutec.Constants;
import com.treutec.Person;

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
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


public final class StreamsProcessor {
  private final Logger logger;
  private final String brokers;
  private final String schemaRegistryUrl;

  public StreamsProcessor(String brokers, String schemaRegistryUrl) {
    super();
    this.brokers = brokers;
    this.schemaRegistryUrl = schemaRegistryUrl;
    this.logger = LogManager.getLogger(this.getClass());
  }

  public final void process() {
    StreamsBuilder streamsBuilder = new StreamsBuilder();
    GenericAvroSerde avroSerde = new GenericAvroSerde();

    avroSerde.configure(
        Collections.singletonMap("schema.registry.url", schemaRegistryUrl), false);

    KStream avroStream = streamsBuilder.stream(
        Constants.getPersonsAvroTopic(), Consumed.with(Serdes.String(), avroSerde));
    
	  KStream personAvroStream = avroStream.mapValues((v -> {
	      GenericRecord personAvro = (GenericRecord) v;
        Person person = new Person(
            personAvro.get("firstName").toString(),
            personAvro.get("lastName").toString(),
            new Date((Long) personAvro.get("birthDate")),
            personAvro.get("city").toString(),
            personAvro.get("ipAddress").toString()
        );
        logger.debug("Person: " + person);
        return person;
    }));

	KStream ageStream = personAvroStream.map((k, v) -> {
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
    (new StreamsProcessor("localhost:9092", "http://localhost:8081")).process();
  }
}
