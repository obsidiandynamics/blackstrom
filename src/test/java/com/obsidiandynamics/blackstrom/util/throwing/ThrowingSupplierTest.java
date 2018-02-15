package com.obsidiandynamics.blackstrom.util.throwing;

import static org.junit.Assert.*;

import org.junit.*;

public final class ThrowingSupplierTest {
  @Test
  public void testGet() throws Exception {
    final ThrowingSupplier<String> s = () -> "test";
    final String val = s.get();
    assertEquals("test", val);
  }
}
