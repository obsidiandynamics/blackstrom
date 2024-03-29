package com.obsidiandynamics.blackstrom.codec;

import java.io.*;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.std.*;
import com.fasterxml.jackson.databind.node.*;

final class JacksonMonoVariantDeserializer extends StdDeserializer<MonoVariant> {
  private static final long serialVersionUID = 1L;
  
  JacksonMonoVariantDeserializer() {
    super(MonoVariant.class);
  }
  
  @Override
  public MonoVariant deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
    final var thisNode = p.getCodec().<ObjectNode>readTree(p);
    final var contentType = thisNode.get("@contentType").asText();
    final var contentVersion = thisNode.get("@contentVersion").asInt();
    final var nestedContentNode = thisNode.get("@content");
    final JsonNode contentNode;
    if (nestedContentNode != null) {
      // payload was encapsulated, implying that it was an array or a scalar
      contentNode = nestedContentNode;
    } else {
      // content was inlined
      thisNode.remove("@contentType");    // remove properties to prevent problems with downstream deserializers
      thisNode.remove("@contentVersion"); // ...
      contentNode = thisNode;
    }
    
    return new MonoVariant(new ContentHandle(contentType, contentVersion), new JacksonPackedForm(p, contentNode), null);
  }
}
