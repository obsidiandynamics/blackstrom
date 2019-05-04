package com.obsidiandynamics.blackstrom.codec;

import org.junit.*;

import com.obsidiandynamics.verifier.*;

import nl.jqno.equalsverifier.*;

public final class MultiVariantTest {
  @Test
  public void testPojo() {
    PojoVerifier.forClass(MultiVariant.class)
    .constructorArgs(new ConstructorArgs()
                     .with(UniVariant[].class, new UniVariant[] {new UniVariant(new ContentHandle("foo", 0), null, "content")}))
    .verify();
  }
  
  @Test
  public void testEqualsHashCode() {
    EqualsVerifier.forClass(MultiVariant.class).verify();
  }
}
