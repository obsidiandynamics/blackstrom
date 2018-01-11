package com.obsidiandynamics.blackstrom.codec;

import java.io.*;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.std.*;

final class JacksonPayloadDeserializer extends StdDeserializer<Payload> {
  private static final long serialVersionUID = 1L;
  
  JacksonPayloadDeserializer() {
    super(Payload.class);
  }
  
  static final class PayloadDeserializationException extends JsonProcessingException {
    private static final long serialVersionUID = 1L;
    
    PayloadDeserializationException(Throwable cause) {
      super("Error deserializing payload", cause);
    }
  }

  @Override
  public Payload deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
    final JsonNode root = p.getCodec().readTree(p);
    final String payloadClassName = root.get("payloadClass").asText();
    final Class<?> payloadClass;
    try {
      payloadClass = Class.forName(payloadClassName);
    } catch (ClassNotFoundException e) {
      throw new PayloadDeserializationException(e);
    }
    
    final JsonNode payloadNode = root.get("payload");
    final Object payload = p.getCodec().treeToValue(payloadNode, payloadClass);
    return Payload.pack(payload);
  }
}
