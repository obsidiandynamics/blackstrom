package com.obsidiandynamics.blackstrom.codec;

import java.io.*;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;

final class JacksonUtils {
  private JacksonUtils() {}
  
  static void writeString(String fieldName, String value, JsonGenerator gen) throws IOException {
    if (value != null) {
      gen.writeFieldName(fieldName);
      gen.writeString(value);
    }
  }
  
  static void writeObject(String fieldName, Object value, JsonGenerator gen) throws IOException {
    if (value != null) {
      gen.writeFieldName(fieldName);
      gen.writeObject(value);
    }
  }
  
  static String readString(String fieldName, JsonNode node) {
    final JsonNode stringNode = node.get(fieldName);
    return stringNode != null ? stringNode.asText(null) : null;
  }
  
  static <T> T readObject(String fieldName, JsonNode node, JsonParser p, Class<T> type) throws JsonProcessingException {
    final JsonNode objectNode = node.get(fieldName);
    return objectNode != null ? p.getCodec().treeToValue(objectNode, type) : null;
  }
}
