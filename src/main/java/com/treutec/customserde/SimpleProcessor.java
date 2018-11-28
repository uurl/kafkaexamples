package com.treutec.customserde;

import com.treutec.Constants;
import com.treutec.Person;
import java.time.Duration;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public final class SimpleProcessor {
  private Consumer<String, Person> consumer;
  private Producer<String, String> producer;

  public SimpleProcessor(String brokers) {
    Properties consumerProps = new Properties();
    consumerProps.put("bootstrap.servers", brokers);
    consumerProps.put("group.id", "person-processor");
    consumerProps.put("key.deserializer", StringDeserializer.class);
    consumerProps.put("value.deserializer", PersonDeserializer.class);
    consumer = new KafkaConsumer<>(consumerProps);

    Properties producerProps = new Properties();
    producerProps.put("bootstrap.servers", brokers);
    producerProps.put("key.serializer", StringSerializer.class);
    producerProps.put("value.serializer", StringSerializer.class);
    producer = new KafkaProducer<>(producerProps);
  }

  public final void process() {
    consumer.subscribe(Collections.singletonList(Constants.getPersonsTopic()));

    while(true) {
      ConsumerRecords records = consumer.poll(Duration.ofSeconds(1L));

      for(Object record : records) {
        ConsumerRecord it = (ConsumerRecord) record;
        Person person = (Person) it.value();
        LocalDate birthDateLocal =
            person.getBirthDate().toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
        int age = Period.between(birthDateLocal, LocalDate.now()).getYears();
        Future future = producer.send(new ProducerRecord<>(
            Constants.getAgesTopic(), person.getFirstName() + ' ' + person.getLastName(), String.valueOf(age)));
        try {
          future.get();
        } catch (InterruptedException | ExecutionException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public static void main( String[] args) {
    new SimpleProcessor("localhost:9092").process();
  }
}
