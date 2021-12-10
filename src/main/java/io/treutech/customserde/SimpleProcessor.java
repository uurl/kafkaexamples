package io.treutech.customserde;

import io.treutech.Constants;
import io.treutech.Person;
import java.time.Duration;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public final class SimpleProcessor {
  private final Consumer<String, Person> consumer;
  private final Producer<String, String> producer;

  public SimpleProcessor(String brokers) {
    Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "person-processor");
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, PersonDeserializer.class);
    consumer = new KafkaConsumer<>(consumerProps);

    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producer = new KafkaProducer<>(producerProps);
  }

  @SuppressWarnings("InfiniteLoopStatement")
  public void process() {
    consumer.subscribe(Collections.singletonList(Constants.getPersonsTopic()));

    while(true) {
      ConsumerRecords<String, Person> records = consumer.poll(Duration.ofSeconds(1L));

      for(ConsumerRecord<String, Person> record : records) {
        Person person = record.value();
        LocalDate birthDateLocal = person.birthDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
        int age = Period.between(birthDateLocal, LocalDate.now()).getYears();
        Future<RecordMetadata> future = producer.send(new ProducerRecord<>(
            Constants.getAgesTopic(), person.firstName + ' ' + person.lastName, String.valueOf(age)));
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
