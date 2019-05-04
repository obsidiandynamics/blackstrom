package com.obsidiandynamics.blackstrom.codec;

import java.io.*;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.ser.std.*;

final class JacksonMultiVariantSerializer extends StdSerializer<MultiVariant> {
  private static final long serialVersionUID = 1L;

  JacksonMultiVariantSerializer() {
    super(MultiVariant.class);
  }

  @Override
  public void serialize(MultiVariant v, JsonGenerator gen, SerializerProvider provider) throws IOException {
    gen.writeObject(v.getVariants());
  }
}
