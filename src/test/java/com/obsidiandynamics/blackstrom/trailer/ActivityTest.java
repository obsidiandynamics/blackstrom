package com.obsidiandynamics.blackstrom.trailer;

import org.junit.*;

import com.obsidiandynamics.assertion.*;

public final class ActivityTest {
  @Test
  public void test() {
    final Action action = new Action(() -> {});
    Assertions.assertToStringOverride(action);
  }
}
