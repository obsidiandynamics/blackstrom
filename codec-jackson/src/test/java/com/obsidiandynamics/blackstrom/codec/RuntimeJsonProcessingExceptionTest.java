package com.obsidiandynamics.blackstrom.codec;

import org.junit.*;

public final class RuntimeJsonProcessingExceptionTest {
  @Test
  public void testConstructor() {
    final var exception = new RuntimeJsonProcessingException(null);
    Assert.assertNotNull(exception);
  }
}
