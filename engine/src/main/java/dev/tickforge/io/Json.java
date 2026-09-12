package dev.tickforge.io;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.io.IOException;

public final class Json {
  public static final ObjectMapper MAPPER =
      JsonMapper.builder().enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS).build();

  private Json() {}

  public static String encode(Object value) {
    try {
      return MAPPER.writeValueAsString(value);
    } catch (IOException e) {
      throw new IllegalArgumentException("cannot serialize", e);
    }
  }

  public static <T> T decode(String value, Class<T> type) {
    try {
      return MAPPER.readValue(value, type);
    } catch (IOException e) {
      throw new IllegalArgumentException("invalid JSON", e);
    }
  }
}
