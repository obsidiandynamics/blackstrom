package com.obsidiandynamics.blackstrom.codec;

import java.io.*;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.std.*;
import com.fasterxml.jackson.databind.node.*;

final class JacksonMultiVariantDeserializer extends StdDeserializer<MultiVariant> {
  private static final long serialVersionUID = 1L;
  
  JacksonMultiVariantDeserializer() {
    super(MultiVariant.class);
  }
  
  @Override
  public MultiVariant deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
    final var thisNode = p.getCodec().<ArrayNode>readTree(p);
    final var codec = p.getCodec();
    final var variants = codec.treeToValue(thisNode, UniVariant[].class);
//    final var items = thisNode.size();
//    final var variants = new UniVariant[items];
//    final var codec = p.getCodec();
//    for (var i = 0; i < items; i++) {
//      variants[i] = codec.treeToValue(thisNode.get(i), UniVariant.class);
//    }
    return new MultiVariant(variants);
  }
}
