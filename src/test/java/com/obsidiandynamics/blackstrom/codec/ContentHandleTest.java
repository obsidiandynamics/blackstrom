package com.obsidiandynamics.blackstrom.codec;

import org.junit.*;

import com.obsidiandynamics.verifier.*;

import nl.jqno.equalsverifier.*;

public final class ContentHandleTest {
  @Test
  public void testPojo() {
    PojoVerifier.forClass(ContentHandle.class)
    .constructorArgs(new ConstructorArgs()
                     .with(String.class, "test-sys:message")
                     .with(int.class, 0))
    .verify();
  }
  
  @Test
  public void testEqualsHashCode() {
    EqualsVerifier.forClass(ContentHandle.class).verify();
  }
}
