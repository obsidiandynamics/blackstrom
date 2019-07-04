package com.obsidiandynamics.blackstrom.spotter;

import org.junit.*;

import com.obsidiandynamics.verifier.*;

public final class SpotterConfigTest {
  @Test
  public void testGettersAndToString() {
    PojoVerifier.forClass(SpotterConfig.class).excludeMutators().verify();
  }
  
  @Test
  public void testFluent() {
    FluentVerifier.forClass(SpotterConfig.class).verify();
  }
  
  @Test
  public void testValidate_defaults() {
    new SpotterConfig().validate();
  }
}
