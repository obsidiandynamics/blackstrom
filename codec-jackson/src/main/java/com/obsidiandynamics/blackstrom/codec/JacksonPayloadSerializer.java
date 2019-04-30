package com.obsidiandynamics.blackstrom.codec;

import java.io.*;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import com.fasterxml.jackson.databind.ser.std.*;

class JacksonPayloadSerializer extends StdSerializer<Payload> {
  private static final long serialVersionUID = 1L;

  JacksonPayloadSerializer() {
    super(Payload.class);
  }

  @Override
  public void serialize(Payload p, JsonGenerator gen, SerializerProvider provider) throws IOException {
    final var value = p.unpack();
    final var mapper = (ObjectMapper) gen.getCodec();
    final var valueTree = mapper.valueToTree(value);
    if (valueTree instanceof ObjectNode) {
      // can only inline an object node
      ((ObjectNode) valueTree).put("@payloadClass", value.getClass().getName());
      gen.writeTree(valueTree);
    } else {
      // when the value serializes as a scalar or an array, we need to encapsulate it
      gen.writeStartObject();
      gen.writeStringField("@payloadClass", value.getClass().getName());
      gen.writeObjectField("@payload", valueTree);
      gen.writeEndObject();
    }
  }
}
