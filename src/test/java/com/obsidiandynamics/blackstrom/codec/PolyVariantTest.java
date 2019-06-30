package com.obsidiandynamics.blackstrom.codec;

import org.junit.*;

import com.obsidiandynamics.verifier.*;

import nl.jqno.equalsverifier.*;

public final class PolyVariantTest {
  @Test
  public void testPojo() {
    PojoVerifier.forClass(PolyVariant.class)
    .constructorArgs(new ConstructorArgs()
                     .with(MonoVariant[].class, new MonoVariant[] {new MonoVariant(new ContentHandle("foo", 0), null, "content")}))
    .verify();
  }
  
  @Test
  public void testEqualsHashCode() {
    EqualsVerifier.forClass(PolyVariant.class).verify();
  }
}
