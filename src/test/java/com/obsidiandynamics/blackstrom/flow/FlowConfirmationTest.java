package com.obsidiandynamics.blackstrom.flow;

import org.junit.*;

import com.obsidiandynamics.assertion.*;

public final class FlowConfirmationTest {
  @Test
  public void test() {
    final FlowConfirmation c = new FlowConfirmation(() -> {});
    Assertions.assertToStringOverride(c);
  }
}
