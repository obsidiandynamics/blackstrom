package com.obsidiandynamics.blackstrom.codec;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.io.*;

import org.junit.*;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import com.obsidiandynamics.assertion.*;

public final class JacksonUtilsTest {
  @Test
  public void testConformance() {
    Assertions.assertUtilityClassWellDefined(JacksonUtils.class);
  }
  
  @Test
  public void testWriteString() throws IOException {
    final JsonGenerator gen = mock(JsonGenerator.class);
    JacksonUtils.writeString("field", null, gen);
    verifyNoMoreInteractions(gen);
    
    JacksonUtils.writeString("field", "value", gen);
    verify(gen).writeFieldName(eq("field"));
    verify(gen).writeString(eq("value"));
  }
  
  @Test
  public void testWriteObject() throws IOException {
    final JsonGenerator gen = mock(JsonGenerator.class);
    JacksonUtils.writeObject("field", null, gen);
    verifyNoMoreInteractions(gen);
    
    JacksonUtils.writeObject("field", "value", gen);
    verify(gen).writeFieldName(eq("field"));
    verify(gen).writeObject(eq("value"));
  }
  
  @Test
  public void testReadString() {
    final JsonNode node = mock(JsonNode.class);
    when(node.get(eq("field1"))).thenReturn(null);
    when(node.get(eq("field2"))).thenReturn(NullNode.getInstance());
    when(node.get(eq("field3"))).thenReturn(new TextNode("value"));
    
    assertNull(JacksonUtils.readString("field1", node));
    assertNull(JacksonUtils.readString("field2", node));
    assertEquals("value", JacksonUtils.readString("field3", node));
  }
  
  @Test
  public void testReadObject() throws JsonProcessingException {
    final ObjectCodec codec = mock(ObjectCodec.class);
    final JsonParser p = mock(JsonParser.class);
    when(p.getCodec()).thenReturn(codec);
    final JsonNode node = mock(JsonNode.class);
    
    final JsonNode f1 = null;
    final NullNode f2 = NullNode.getInstance();
    final TextNode f3 = new TextNode("value");
    when(node.get(eq("field1"))).thenReturn(f1);
    when(node.get(eq("field2"))).thenReturn(f2);
    when(node.get(eq("field3"))).thenReturn(f3);
    when(codec.treeToValue(eq(f2), eq(String.class))).thenReturn(f2.asText(null));
    when(codec.treeToValue(eq(f3), eq(String.class))).thenReturn(f3.asText());

    assertNull(JacksonUtils.readObject("field1", node, p, String.class));
    assertNull(JacksonUtils.readObject("field2", node, p, String.class));
    assertEquals("value", JacksonUtils.readObject("field3", node, p, String.class));
  }
}
