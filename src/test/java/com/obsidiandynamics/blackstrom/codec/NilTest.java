package com.obsidiandynamics.blackstrom.codec;

import static org.junit.Assert.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;
import com.obsidiandynamics.verifier.*;

import nl.jqno.equalsverifier.*;

public final class NilTest {
  @Test
  public void testSingleton() {
    assertSame(Nil.getInstance(), Nil.getInstance());
  }
  
  @Test
  public void testPojo() {
    PojoVerifier.forClass(Nil.class).verify();
    Assertions.assertToStringOverride(Nil.getInstance());
  }
  
  @Test
  public void testEqualsHashCode() {
    EqualsVerifier.forClass(Nil.class).verify();
  }
  
  @Test
  public void testGetContentHandle() {
    assertEquals(new ContentHandle("std:nil", 1), Nil.getContentHandle());
  }
  
  @Test
  public void testCapture() {
    assertSame(Nil.getInstance(), Nil.capture().getContent());
  }
}
