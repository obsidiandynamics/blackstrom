package com.obsidiandynamics.blackstrom.codec;

import java.io.*;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.ser.std.*;

class JacksonPayloadSerializer extends StdSerializer<Payload> {
  private static final long serialVersionUID = 1L;

  JacksonPayloadSerializer() {
    super(Payload.class);
  }

  @Override
  public void serialize(Payload p, JsonGenerator gen, SerializerProvider provider) throws IOException {
    gen.writeStartObject();
    final Object value = p.unpack();
    gen.writeStringField("payloadClass", value.getClass().getName());
    gen.writeObjectField("payload", value);
    gen.writeEndObject();
  }
}
