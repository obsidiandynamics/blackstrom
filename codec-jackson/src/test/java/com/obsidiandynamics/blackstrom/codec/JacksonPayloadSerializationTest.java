package com.obsidiandynamics.blackstrom.codec;

import static org.junit.Assert.*;

import java.io.*;
import java.util.*;

import org.hamcrest.core.*;
import org.junit.*;
import org.junit.rules.*;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.*;
import com.obsidiandynamics.blackstrom.codec.JacksonPayloadDeserializer.*;
import com.obsidiandynamics.zerolog.*;

public final class JacksonPayloadSerializationTest {
  private static final Zlg zlg = Zlg.forDeclaringClass().get();
  
  private static ObjectMapper createObjectMapper() {
    final var mapper = new ObjectMapper();
    final var m = new SimpleModule();
    m.addSerializer(Payload.class, new JacksonPayloadSerializer());
    m.addDeserializer(Payload.class, new JacksonPayloadDeserializer());
    mapper.registerModule(m);
    return mapper;
  }
  
  private static void logEncoded(String encoded) {
    zlg.t("encoded %s", z -> z.arg(encoded));
  }
  
  private static final class TestClass {
    @JsonProperty
    private final String a;

    @JsonProperty
    private final int b;
    
    TestClass(@JsonProperty("a") String a, @JsonProperty("b") int b) {
      this.a = a;
      this.b = b;
    }
  }
  
  @Rule 
  public ExpectedException thrown = ExpectedException.none();
  
  @Test
  public void testScalar() throws IOException {
    final var mapper = createObjectMapper();
    final var p = Payload.pack("scalar");
    
    final var encoded = mapper.writeValueAsString(p);
    logEncoded(encoded);
    
    final var d = mapper.readValue(encoded, Payload.class);
    assertEquals(p, d);
  }

  @Test
  public void testArray() throws IOException {
    final var mapper = createObjectMapper();
    final var written = new long[] {0, 1, 3};
    final var p = Payload.pack(written);
    
    final var encoded = mapper.writeValueAsString(p);
    logEncoded(encoded);
    
    final var d = mapper.readValue(encoded, Payload.class);
    final var read = d.<long[]>unpack();
    assertArrayEquals(written, read);
  }

  @Test
  public void testMap() throws IOException {
    final var mapper = createObjectMapper();
    final var map = new TreeMap<String, List<String>>();
    map.put("a", Arrays.asList("a", "A"));
    map.put("b", Arrays.asList("a", "B"));
    final var p = Payload.pack(map);
    
    final var encoded = mapper.writeValueAsString(p);
    logEncoded(encoded);
    
    final var d = mapper.readValue(encoded, Payload.class);
    assertEquals(p, d);
  }

  @Test
  public void testObject() throws IOException {
    final var mapper = createObjectMapper();
    final var written = new TestClass("someString", 42);
    final var p = Payload.pack(written);
    
    final var encoded = mapper.writeValueAsString(p);
    logEncoded(encoded);
    
    final var d = mapper.readValue(encoded, Payload.class);
    final var read = d.<TestClass>unpack();
    assertEquals(written.a, read.a);
    assertEquals(written.b, read.b);
  }
  
  @Test
  public void testInvalidClass() throws IOException {
    final var mapper = createObjectMapper();
    final var encoded = "{\"@payloadClass\":\"com.foo.bar.Baz\",\"@payload\":[0,1,3]}";
    
    thrown.expect(PayloadDeserializationException.class);
    thrown.expectCause(IsInstanceOf.instanceOf(ClassNotFoundException.class));
    mapper.readValue(encoded, Payload.class);
  }
}
