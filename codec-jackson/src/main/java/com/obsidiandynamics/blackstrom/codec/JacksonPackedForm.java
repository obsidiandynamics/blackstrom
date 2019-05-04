package com.obsidiandynamics.blackstrom.codec;

import static com.obsidiandynamics.func.Functions.*;

import java.util.*;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;

final class JacksonPackedForm implements PackedForm {
  private final JsonParser parser;
  
  private final JsonNode node;

  JacksonPackedForm(JsonParser parser, JsonNode node) {
    this.parser = mustExist(parser, "JSON parser cannot be null");
    this.node = mustExist(node, "JSON node cannot be null");
  }
  
  public JsonParser getParser() {
    return parser;
  }
  
  public JsonNode getNode() {
    return node;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj instanceof JacksonPackedForm) {
      final var that = (JacksonPackedForm) obj;
      return Objects.equals(node, that.node);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(node);
  }

  @Override
  public String toString() {
    return JacksonPackedForm.class.getSimpleName() + " [parser=" + parser + ", node=" + node + "]";
  }
}
