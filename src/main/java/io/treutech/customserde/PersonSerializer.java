package io.treutech.customserde;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.treutech.Constants;
import java.util.Map;

import io.treutech.Person;
import org.apache.kafka.common.serialization.Serializer;

public final class PersonSerializer implements Serializer<Person> {

  @Override
  public byte[] serialize(String topic, Person data) {
    if (data == null) {
      return null;
    }
    try {
      return Constants.getJsonMapper().writeValueAsBytes(data);
    } catch (JsonProcessingException e) {
      return null;
    }
  }

  @Override
  public void close() {}

  @Override
  public void configure(Map configs, boolean isKey) {}
}
