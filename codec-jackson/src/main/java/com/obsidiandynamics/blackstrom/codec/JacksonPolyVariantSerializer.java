package com.obsidiandynamics.blackstrom.codec;

import java.io.*;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.ser.std.*;

final class JacksonPolyVariantSerializer extends StdSerializer<PolyVariant> {
  private static final long serialVersionUID = 1L;

  JacksonPolyVariantSerializer() {
    super(PolyVariant.class);
  }

  @Override
  public void serialize(PolyVariant v, JsonGenerator gen, SerializerProvider provider) throws IOException {
    gen.writeObject(v.getVariants());
  }
}
