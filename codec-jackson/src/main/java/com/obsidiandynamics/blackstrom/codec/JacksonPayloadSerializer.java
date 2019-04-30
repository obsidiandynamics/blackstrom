package com.obsidiandynamics.blackstrom.codec;

import java.io.*;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import com.fasterxml.jackson.databind.ser.std.*;

final class JacksonPayloadSerializer extends StdSerializer<Payload> {
  private static final long serialVersionUID = 1L;

  JacksonPayloadSerializer() {
    super(Payload.class);
  }

  @Override
  public void serialize(Payload p, JsonGenerator gen, SerializerProvider provider) throws IOException {
    final var value = p.unpack();
    final var mapper = (ObjectMapper) gen.getCodec();
    final var thisNode = JsonNodeFactory.instance.objectNode();
    final var valueNode = mapper.valueToTree(value);
    thisNode.put("@payloadClass", value.getClass().getName());
    if (valueNode instanceof ObjectNode) {
      // can only inline an object node
      thisNode.setAll((ObjectNode) valueNode);
    } else {
      // when the value serializes as a scalar or an array, we need to encapsulate it
      thisNode.set("@payload", valueNode);
    }
    gen.writeTree(thisNode);
  }
}
