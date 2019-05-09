package com.obsidiandynamics.blackstrom.codec;

import java.io.*;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.ser.std.*;

final class JacksonNilSerializer extends StdSerializer<Nil> {
  private static final long serialVersionUID = 1L;

  JacksonNilSerializer() {
    super(Nil.class);
  }

  @Override
  public void serialize(Nil v, JsonGenerator gen, SerializerProvider provider) throws IOException {
    gen.writeStartObject();
    gen.writeEndObject();
  }
}
