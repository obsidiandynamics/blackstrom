package com.obsidiandynamics.blackstrom.util;

import static junit.framework.TestCase.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;

public final class CastTest {
  @Test
  public void testConformance() throws Exception {
    Assertions.assertUtilityClassWellDefined(Cast.class);
  }

  @Test
  public void testCast() {
    final String s = Cast.from((Object) "hello");
    assertEquals("hello", s);
  }
}
