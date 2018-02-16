package com.obsidiandynamics.blackstrom.util.throwing;

import static org.junit.Assert.*;

import org.junit.*;

public final class ThrowingFunctionTest {
  private static int timesTwo(int input) {
    return input * 2;
  }
  
  @Test
  public void testApply() throws Exception {
    final ThrowingFunction<String, Integer> f0 = Integer::parseInt;
    final CheckedFunction<String, Integer, Exception> f1 = f0.andThen(ThrowingFunctionTest::timesTwo);
    final CheckedFunction<byte[], Integer, Exception> f2 = f1.compose(String::new);
    assertEquals(20, (int) f2.apply("10".getBytes()));
  }
}
