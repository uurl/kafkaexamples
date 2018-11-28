package com.treutec.customserde;

import com.treutec.Constants;
import com.treutec.Person;
import java.io.IOException;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;

public final class PersonDeserializer implements Deserializer {

  @Override
  public Person deserialize(String topic, byte[] data) {
    if (data == null) {
      return null;
    }
    try {
      return Constants.getJsonMapper().readValue(data, Person.class);
    } catch (IOException e) {
      return null;
    }
  }

  @Override
  public void close() {}

  @Override
  public void configure(Map configs, boolean isKey) {}
}

