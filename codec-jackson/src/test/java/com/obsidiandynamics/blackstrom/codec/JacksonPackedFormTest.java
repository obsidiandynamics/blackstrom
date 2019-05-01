package com.obsidiandynamics.blackstrom.codec;

import java.io.*;

import org.junit.*;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.core.base.*;
import com.fasterxml.jackson.core.io.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import com.obsidiandynamics.verifier.*;

import nl.jqno.equalsverifier.*;

public final class JacksonPackedFormTest {
  private static final class DummyParser extends ParserBase {
    DummyParser(IOContext ctxt, int features) {
      super(ctxt, features);
    }

    @Override
    protected void _closeInput() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public JsonToken nextToken() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getText() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public char[] getTextCharacters() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getTextLength() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getTextOffset() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public ObjectCodec getCodec() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setCodec(ObjectCodec c) {
      throw new UnsupportedOperationException();
    }
  }
  
  @Test
  public void testPojo() {
    PojoVerifier.forClass(JacksonPackedForm.class)
    .constructorArgs(new ConstructorArgs()
                     .with(JsonParser.class, new DummyParser(new IOContext(null, null, false), 0))
                     .with(JsonNode.class, JsonNodeFactory.instance.nullNode()))
    .verify();
  }
  
  @Test
  public void testEqualsHashCode() {
    EqualsVerifier.forClass(JacksonPackedForm.class)
    .withPrefabValues(JsonNode.class, JsonNodeFactory.instance.textNode("red"), JsonNodeFactory.instance.textNode("black"))
    .withIgnoredFields("parser").verify();
  }
}
