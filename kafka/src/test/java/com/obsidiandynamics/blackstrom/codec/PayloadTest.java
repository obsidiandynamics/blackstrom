package com.obsidiandynamics.blackstrom.codec;

import static org.junit.Assert.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;

public final class PayloadTest {
  @Test
  public void testToString() {
    Assertions.assertToStringOverride(Payload.pack("value"));
  }

  @Test
  public void testPackNull() {
    assertNull(Payload.pack(null));
  }
  
  @Test
  public void testPackNotNull() {
    final Payload p = Payload.pack("value");
    assertNotNull(p);
    assertEquals("value", p.unpack());
  }
  
  @Test
  public void testUnpackNull() {
    assertNull(Payload.unpack(null));
  }
  
  @Test
  public void testUnpackNotNull() {
    assertEquals("value", Payload.unpack(Payload.pack("value")));
  }
  
  @Test
  public void testUnpackNonPayload() {
    assertEquals("value", Payload.unpack("value"));
  }
}
