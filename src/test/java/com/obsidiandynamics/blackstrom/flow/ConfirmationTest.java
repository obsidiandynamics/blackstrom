package com.obsidiandynamics.blackstrom.flow;

import org.junit.*;

import com.obsidiandynamics.assertion.*;

public final class ConfirmationTest {
  @Test
  public void test() {
    final Confirmation c = new Confirmation(() -> {});
    Assertions.assertToStringOverride(c);
  }
}
