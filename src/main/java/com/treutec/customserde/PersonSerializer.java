package com.treutec.customserde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.treutec.Constants;
import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;

public final class PersonSerializer implements Serializer {

  @Override
  public byte[] serialize(String topic, Object data) {
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
