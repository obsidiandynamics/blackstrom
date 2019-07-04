package com.obsidiandynamics.blackstrom.codec;

import static org.junit.Assert.*;

import org.assertj.core.api.*;
import org.junit.*;

import com.obsidiandynamics.verifier.*;

import nl.jqno.equalsverifier.*;

public final class MonoVariantTest {
  @Test
  public void testPojo() {
    PojoVerifier.forClass(MonoVariant.class)
    .constructorArgs(new ConstructorArgs()
                     .with(ContentHandle.class, new ContentHandle("type", 1))
                     .with(PackedForm.class, new IdentityPackedForm(null))
                     .with(Object.class, null))
    .verify();
    
    PojoVerifier.forClass(MonoVariant.class)
    .constructorArgs(new ConstructorArgs()
                     .with(ContentHandle.class, new ContentHandle("type", 1))
                     .with(PackedForm.class, null)
                     .with(Object.class, "content"))
    .verify();
  }
  
  @Test
  public void testEqualsHashCode() {
    EqualsVerifier.forClass(MonoVariant.class).verify();
  }
  
  @Test
  public void testConstructor_nestedVariant() {
    final var poly = new PolyVariant(new MonoVariant(new ContentHandle("inner", 0), null, "someContent"));
    Assertions.assertThatThrownBy(() -> {
      new MonoVariant(new ContentHandle("outer", 0), null, poly);
    }).isInstanceOf(IllegalArgumentException.class).hasMessage("Cannot nest content of type Variant");
  }
  
  @Test
  public void testConstructor_bothPackedAndContentSupplied() {
    Assertions.assertThatThrownBy(() -> {
      new MonoVariant(new ContentHandle("outer", 0), new IdentityPackedForm("someContent"), "someContent");
    }).isInstanceOf(IllegalArgumentException.class).hasMessage("Either the packed form or the original content must be specified");
  }
  
  @Test
  public void testMap_withExistingContent() {
    final var v = new MonoVariant(new ContentHandle("outer", 0), null, "someContent");
    final var mapper = new ContentMapper();
    assertEquals(v.<String>getContent(), v.<String>map(mapper));
  }
}
