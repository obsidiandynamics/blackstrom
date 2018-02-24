package com.obsidiandynamics.blackstrom.util.props;

import static org.junit.Assert.*;

import java.util.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;

public final class PropertyManipTest {
  @Test
  public void testConformance() throws Exception {
    Assertions.assertUtilityClassWellDefined(PropertyManip.class);
  }
  
  @Test
  public void testMerge() {
    final Properties a = new Properties();
    a.setProperty("a", "A");
    final Properties b = new Properties();
    b.setProperty("b", "B");
    final Properties merged = PropertyManip.mergeProps(a, b);
    assertEquals("A", merged.getProperty("a"));
    assertEquals("B", merged.getProperty("b"));
  }
}
