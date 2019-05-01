package com.obsidiandynamics.blackstrom.codec;

import org.junit.*;

import com.obsidiandynamics.verifier.*;

import nl.jqno.equalsverifier.*;

public final class VersionableTest {
  @Test
  public void testPojo() {
    PojoVerifier.forClass(Versionable.class)
    .constructorArgs(new ConstructorArgs()
                     .with(ContentHandle.class, new ContentHandle("type", 1))
                     .with(PackedForm.class, new IdentityPackedForm(null))
                     .with(Object.class, null))
    .verify();
    
    PojoVerifier.forClass(Versionable.class)
    .constructorArgs(new ConstructorArgs()
                     .with(ContentHandle.class, new ContentHandle("type", 1))
                     .with(PackedForm.class, null)
                     .with(Object.class, "content"))
    .verify();
  }
  
  @Test
  public void testEqualsHashCode() {
    EqualsVerifier.forClass(Versionable.class).verify();
  }
}
