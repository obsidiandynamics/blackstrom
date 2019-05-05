package com.obsidiandynamics.blackstrom.codec;

import static org.junit.Assert.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;

import nl.jqno.equalsverifier.*;

public final class PayloadTest {
  @Test
  public void testToString() {
    Assertions.assertToStringOverride(Payload.pack("value"));
  }

  @Test
  public void testPack_null() {
    assertNull(Payload.pack(null));
  }
  
  @Test
  public void testPack_notNull() {
    final var p = Payload.pack("value");
    assertNotNull(p);
    assertEquals("value", p.unpack());
  }
  
  @Test
  public void testPack_payload() {
    final var inner = Payload.pack("value");
    org.assertj.core.api.Assertions.assertThatThrownBy(() -> {
      Payload.pack(inner);
    }).isInstanceOf(IllegalArgumentException.class).hasMessage("Cannot nest payload of type Payload");
  }
  
  @Test
  public void testUnpack_null() {
    assertNull(Payload.unpack(null));
  }
  
  @Test
  public void testUnpack_notNull() {
    assertEquals("value", Payload.unpack(Payload.pack("value")));
  }
  
  @Test
  public void testUnpack_nonPayload() {
    assertEquals("value", Payload.unpack("value"));
  }
  
  @Test
  public void testEqualsHashCode() {
    EqualsVerifier.forClass(Payload.class).verify();
  }
}
