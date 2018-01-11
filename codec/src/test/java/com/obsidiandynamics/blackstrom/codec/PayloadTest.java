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
  
  @Test
  public void testEqualsHashCode() {
    final Payload p1 = Payload.pack("value");
    final Payload p2 = Payload.pack("foo");
    final Payload p3 = Payload.pack("value");
    final Payload p4 = p1;
    
    assertNotEquals(p1, p2);
    assertEquals(p1, p3);
    assertEquals(p1, p4);
    assertNotEquals(p1, new Object());

    assertNotEquals(p1.hashCode(), p2.hashCode());
    assertEquals(p1.hashCode(), p3.hashCode());
  }
}
