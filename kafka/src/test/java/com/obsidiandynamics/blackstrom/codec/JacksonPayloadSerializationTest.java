package com.obsidiandynamics.blackstrom.codec;

import static org.junit.Assert.*;

import java.io.*;
import java.util.*;

import org.hamcrest.core.*;
import org.junit.*;
import org.junit.rules.*;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.*;
import com.obsidiandynamics.blackstrom.codec.JacksonPayloadDeserializer.*;
import com.obsidiandynamics.indigo.util.*;

public final class JacksonPayloadSerializationTest implements TestSupport {
  private static ObjectMapper createObjectMapper() {
    final ObjectMapper mapper = new ObjectMapper();
    final SimpleModule m = new SimpleModule();
    m.addSerializer(Payload.class, new JacksonPayloadSerializer());
    m.addDeserializer(Payload.class, new JacksonPayloadDeserializer());
    mapper.registerModule(m);
    return mapper;
  }
  
  private static void logEncoded(String encoded) {
    if (LOG) LOG_STREAM.format("encoded %s\n", encoded);
  }
  
  @Rule 
  public ExpectedException thrown = ExpectedException.none();
  
  @Test
  public void testScalar() throws IOException {
    final ObjectMapper mapper = createObjectMapper();
    final Payload p = Payload.pack("scalar");
    
    final String encoded = mapper.writeValueAsString(p);
    logEncoded(encoded);
    
    final Payload d = mapper.readValue(encoded, Payload.class);
    assertEquals(p, d);
  }

  @Test
  public void testArray() throws IOException {
    final ObjectMapper mapper = createObjectMapper();
    final Payload p = Payload.pack(new long[] {0, 1, 3});
    
    final String encoded = mapper.writeValueAsString(p);
    logEncoded(encoded);
    
    final Payload d = mapper.readValue(encoded, Payload.class);
    assertEquals(p, d);
  }

  @Test
  public void testMap() throws IOException {
    final ObjectMapper mapper = createObjectMapper();
    final Map<String, List<String>> map = new TreeMap<>();
    map.put("a", Arrays.asList("a", "A"));
    map.put("b", Arrays.asList("a", "B"));
    final Payload p = Payload.pack(map);
    
    final String encoded = mapper.writeValueAsString(p);
    logEncoded(encoded);
    
    final Payload d = mapper.readValue(encoded, Payload.class);
    assertEquals(p, d);
  }
  
  @Test
  public void testInvalidClass() throws IOException {
    final ObjectMapper mapper = createObjectMapper();
    final String encoded = "{\"payloadClass\":\"com.foo.bar.Baz\",\"payload\":[0,1,3]}";
    
    thrown.expect(PayloadDeserializationException.class);
    thrown.expectCause(IsInstanceOf.instanceOf(ClassNotFoundException.class));
    mapper.readValue(encoded, Payload.class);
  }
}
