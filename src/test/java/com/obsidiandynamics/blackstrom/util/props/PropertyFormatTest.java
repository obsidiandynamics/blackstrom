package com.obsidiandynamics.blackstrom.util.props;

import static org.junit.Assert.*;

import java.util.*;
import java.util.function.*;
import java.util.regex.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;

public final class PropertyFormatTest {
  @Test
  public void testConformance() throws Exception {
    Assertions.assertUtilityClassWellDefined(PropertyFormat.class);
  }
  
  @Test
  public void testLeftPad() {
    assertEquals("  x", PropertyFormat.leftPad(3).apply("x"));
  }
  
  @Test
  public void testRightPad() {
    assertEquals("x  ", PropertyFormat.rightPad(3).apply("x"));
  }
  
  @Test
  public void testDefaultLeftPad() {
    assertTrue(PropertyFormat.defaultPad() != 0);
    assertEquals(PropertyFormat.leftPad(PropertyFormat.defaultPad()).apply("x"), 
                 PropertyFormat.leftPad().apply("x"));
  }
  
  @Test
  public void testDefaultRightPad() {
    assertTrue(PropertyFormat.defaultPad() != 0);
    assertEquals(PropertyFormat.rightPad(PropertyFormat.defaultPad()).apply("x"), 
                 PropertyFormat.rightPad().apply("x"));
  }
  
  @Test
  public void testPrefix() {
    assertEquals("_x", PropertyFormat.prefix("_").apply("x"));
  }
  
  @Test
  public void testSuffix() {
    assertEquals("x_", PropertyFormat.suffix("_").apply("x"));
  }
  
  @Test
  public void testStartsWith() {
    assertTrue(PropertyFormat.startsWith("_").test("_x"));
    assertFalse(PropertyFormat.startsWith("_").test("x"));
  }
  
  @Test
  public void testAny() {
    assertTrue(PropertyFormat.any().test("foo"));
  }
  
  @Test
  public void printProps() {
    final StringBuilder logLine = new StringBuilder();
    final Properties props = new Properties();
    props.setProperty("a", "A");
    props.setProperty("b", "B");
    PropertyFormat.printProps(s -> logLine.append(s).append('\n'), props, 
                              Function.identity(), Function.identity(), PropertyFormat.any());
    assertEquals(2, lines(logLine));
  }
  
  @Test
  public void printPropsNoMatch() {
    final StringBuilder logLine = new StringBuilder();
    final Properties props = new Properties();
    props.setProperty("a", "A");
    props.setProperty("b", "B");
    PropertyFormat.printProps(s -> logLine.append(s).append('\n'), props, 
                              Function.identity(), Function.identity(), s -> false);
    assertEquals(0, logLine.length());
  }
  
  private static int lines(StringBuilder sb) {
    final Matcher matcher = Pattern.compile("\n").matcher(sb);
    int count = 0;
    while (matcher.find()) count++;
    return count;
  }
  
  @Test
  public void printTitle() {
    final StringBuilder logLine = new StringBuilder();
    PropertyFormat.printTitle(logLine::append, null);
    assertEquals(0, logLine.length());

    PropertyFormat.printTitle(s -> logLine.append(s).append('\n'), "foo");
    assertEquals(1, lines(logLine));
  }
  
  @Test
  public void printStandard() {
    final StringBuilder logLine = new StringBuilder();
    final Properties props = new Properties();
    props.setProperty("a", "A");
    props.setProperty("b", "B");
    PropertyFormat.printStandard(s -> logLine.append(s).append('\n'), "title", props, "");
    assertEquals(3, lines(logLine));
  }
}
