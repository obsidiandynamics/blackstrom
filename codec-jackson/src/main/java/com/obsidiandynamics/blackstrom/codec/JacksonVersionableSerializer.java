package com.obsidiandynamics.blackstrom.codec;

import static com.obsidiandynamics.func.Functions.*;

import java.io.*;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import com.fasterxml.jackson.databind.ser.std.*;

final class JacksonVersionableSerializer extends StdSerializer<Versionable> {
  private static final long serialVersionUID = 1L;

  JacksonVersionableSerializer() {
    super(Versionable.class);
  }

  @Override
  public void serialize(Versionable v, JsonGenerator gen, SerializerProvider provider) throws IOException {
    final JsonNode packedNode;
    if (v.getPacked() != null) {
      // already packed – just write out the tree
      final var packed = mustBeSubtype(v.getPacked(), JacksonPackedForm.class, 
                                       withMessage(() -> "Unsupported packed form: " + v.getPacked().getClass().getSimpleName(), 
                                                   IllegalStateException::new));
      packedNode = packed.getNode();
    } else {
      // not yet packed — need to serialize the content
      final var mapper = (ObjectMapper) gen.getCodec();
      packedNode = mapper.valueToTree(v.getContent());
    }
    
    final var thisNode = JsonNodeFactory.instance.objectNode();
    thisNode.put("@contentType", v.getHandle().getType());
    thisNode.put("@contentVersion", v.getHandle().getVersion());
    if (packedNode instanceof ObjectNode) {
      // can only inline an object node
      thisNode.setAll((ObjectNode) packedNode);
    } else {
      // when the value serializes as a scalar or an array, we need to encapsulate it
      thisNode.set("@content", packedNode);
    }
    
    gen.writeTree(thisNode);
  }
}
