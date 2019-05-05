package com.obsidiandynamics.blackstrom.codec;

import org.assertj.core.api.*;
import org.junit.*;

import com.obsidiandynamics.verifier.*;

import nl.jqno.equalsverifier.*;

public final class UniVariantTest {
  @Test
  public void testPojo() {
    PojoVerifier.forClass(UniVariant.class)
    .constructorArgs(new ConstructorArgs()
                     .with(ContentHandle.class, new ContentHandle("type", 1))
                     .with(PackedForm.class, new IdentityPackedForm(null))
                     .with(Object.class, null))
    .verify();
    
    PojoVerifier.forClass(UniVariant.class)
    .constructorArgs(new ConstructorArgs()
                     .with(ContentHandle.class, new ContentHandle("type", 1))
                     .with(PackedForm.class, null)
                     .with(Object.class, "content"))
    .verify();
  }
  
  @Test
  public void testEqualsHashCode() {
    EqualsVerifier.forClass(UniVariant.class).verify();
  }
  
  @Test
  public void testConstructor_nestedVariant() {
    final var multi = new MultiVariant(new UniVariant(new ContentHandle("inner", 0), null, "someContent"));
    Assertions.assertThatThrownBy(() -> {
      new UniVariant(new ContentHandle("outer", 0), null, multi);
    }).isInstanceOf(IllegalArgumentException.class).hasMessage("Cannot nest content of type Variant");
  }
  
  @Test
  public void testConstructor_bothPackedAndContentSupplied() {
    Assertions.assertThatThrownBy(() -> {
      new UniVariant(new ContentHandle("outer", 0), new IdentityPackedForm("someContent"), "someContent");
    }).isInstanceOf(IllegalArgumentException.class).hasMessage("Either the packed form or the original content must be specified");
  }
}
