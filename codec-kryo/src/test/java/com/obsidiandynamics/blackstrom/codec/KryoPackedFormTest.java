package com.obsidiandynamics.blackstrom.codec;

import org.junit.*;

import com.obsidiandynamics.verifier.*;

import nl.jqno.equalsverifier.*;

public final class KryoPackedFormTest {
  @Test
  public void testPojo() {
    PojoVerifier.forClass(KryoPackedForm.class).verify();
  }
  
  @Test
  public void testEqualsHashCode() {
    EqualsVerifier.forClass(KryoPackedForm.class).verify();
  }
}
