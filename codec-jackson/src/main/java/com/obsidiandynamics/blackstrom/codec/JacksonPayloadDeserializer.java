package com.obsidiandynamics.blackstrom.codec;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.std.*;

final class JacksonPayloadDeserializer extends StdDeserializer<Payload> {
  private static final long serialVersionUID = 1L;
  
  private final Map<String, Class<?>> classCache = new ConcurrentHashMap<>();
  
  JacksonPayloadDeserializer() {
    super(Payload.class);
  }
  
  static final class PayloadDeserializationException extends JsonProcessingException {
    private static final long serialVersionUID = 1L;
    
    PayloadDeserializationException(Throwable cause) {
      super("Error deserializing payload", cause);
    }
  }
  
  private Class<?> classForName(String className) throws PayloadDeserializationException {
    final Class<?> existing = classCache.get(className);
    if (existing != null) {
      return existing;
    } else {
      final Class<?> newClass;
      try {
        newClass = Class.forName(className);
      } catch (ClassNotFoundException e) {
        throw new PayloadDeserializationException(e);
      }
      classCache.put(className, newClass);
      return newClass;
    }
  }

  @Override
  public Payload deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
    final JsonNode root = p.getCodec().readTree(p);
    final String payloadClassName = root.get("payloadClass").asText();
    final Class<?> payloadClass = classForName(payloadClassName);
    
    final JsonNode payloadNode = root.get("payload");
    final Object payload = p.getCodec().treeToValue(payloadNode, payloadClass);
    return Payload.pack(payload);
  }
}
