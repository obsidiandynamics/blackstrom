package com.obsidiandynamics.blackstrom.codec;

import java.io.*;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.std.*;
import com.fasterxml.jackson.databind.node.*;

final class JacksonPolyVariantDeserializer extends StdDeserializer<PolyVariant> {
  private static final long serialVersionUID = 1L;
  
  JacksonPolyVariantDeserializer() {
    super(PolyVariant.class);
  }
  
  @Override
  public PolyVariant deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
    final var thisNode = p.getCodec().<ArrayNode>readTree(p);
    final var codec = p.getCodec();
    final var variants = codec.treeToValue(thisNode, MonoVariant[].class);
    return new PolyVariant(variants);
  }
}
