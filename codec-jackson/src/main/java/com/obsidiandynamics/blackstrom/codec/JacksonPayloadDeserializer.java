package com.obsidiandynamics.blackstrom.codec;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.std.*;
import com.fasterxml.jackson.databind.node.*;

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
    final var existing = classCache.get(className);
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
    final var thisNode = p.getCodec().<ObjectNode>readTree(p);
    final var payloadClassName = thisNode.get("@payloadClass").asText();
    final var payloadNode = thisNode.get("@payload");
    final var payloadClass = classForName(payloadClassName);
    final Object payload;
    if (payloadNode != null) {
      // payload was encapsulated, implying that it was an array or a scalar
      payload = p.getCodec().treeToValue(payloadNode, payloadClass);
    } else {
      // payload was inlined
      thisNode.remove("@payloadClass"); // remove property to prevent problems with downstream deserializers
      payload = p.getCodec().treeToValue(thisNode, payloadClass);
    }
    
    return Payload.pack(payload);
  }
}
