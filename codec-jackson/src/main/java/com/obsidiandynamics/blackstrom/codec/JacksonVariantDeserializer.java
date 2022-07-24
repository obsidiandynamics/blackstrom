package com.obsidiandynamics.blackstrom.codec;

import java.io.*;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.std.*;

final class JacksonVariantDeserializer extends StdDeserializer<Variant> {
  private static final long serialVersionUID = 1L;
  
  JacksonVariantDeserializer() {
    super(Variant.class);
  }
  
  @Override
  public Variant deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
    final var thisNode = p.getCodec().readTree(p);
    final var codec = p.getCodec();
    final var targetClass = thisNode.isArray() ? PolyVariant.class : MonoVariant.class;
    return codec.treeToValue(thisNode, targetClass);
  }
}
