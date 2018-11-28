package com.treutec.withavro;

import com.github.javafaker.Faker;
import com.treutec.Constants;
import com.treutec.Person;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.GenericData.Record;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public final class SimpleProducer {

  private final Producer<String, GenericRecord> producer;
  private Schema schema;

  public SimpleProducer(String brokers, String schemaRegistryUrl) {
    Properties props = new Properties();
    props.put("bootstrap.servers", brokers);
    props.put("key.serializer", StringSerializer.class);
    props.put("value.serializer", KafkaAvroSerializer.class);
    props.put("schema.registry.url", schemaRegistryUrl);
    producer = new KafkaProducer<>(props);
    try {
      schema = (new Parser()).parse(new File("src/main/resources/person.avsc"));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public final void produce(int ratePerSecond) {
    long waitTimeBetweenIterationsMs = 1000L / (long)ratePerSecond;
    Faker faker = new Faker();

    while(true) {
      Person fakePerson = new Person(
          faker.name().firstName(),
          faker.name().lastName(),
          faker.date().birthday(),
          faker.address().city(),
          faker.internet().ipV4Address());
      GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema);
      recordBuilder.set("firstName", fakePerson.getFirstName());
      recordBuilder.set("lastName", fakePerson.getLastName());
      recordBuilder.set("birthDate", fakePerson.getBirthDate().getTime());
      Record avroPerson = recordBuilder.build();
      Future futureResult = producer.send(new ProducerRecord<>(Constants.getPersonsAvroTopic(), avroPerson));
      try {
        Thread.sleep(waitTimeBetweenIterationsMs);
        futureResult.get();
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
    }
  }

  public static void main( String[] args) {
    new SimpleProducer("localhost:9092", "http://localhost:8081").produce(2);
  }
}