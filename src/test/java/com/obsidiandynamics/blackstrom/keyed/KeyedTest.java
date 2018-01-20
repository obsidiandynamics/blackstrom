package com.obsidiandynamics.blackstrom.keyed;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.*;
import java.util.function.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;

public final class KeyedTest {
  private interface StringFactory extends Function<String, String> {}
  
  private static StringFactory mockSupplier() {
    final StringFactory factory = mock(StringFactory.class);
    when(factory.apply(any())).thenReturn("value");
    return factory;
  }
  
  @Test
  public void testSingleCheckedAbsent() {
    final Map<String, String> map = new HashMap<>();
    final StringFactory factory = mockSupplier();
    final String value = Keyed.getOrSet(this, map, "key", factory);
    assertEquals("value", value);
    verify(factory).apply(eq("key"));
  }
  
  @Test
  public void testSingleCheckedExisting() {
    final Map<String, String> map = new HashMap<>();
    map.put("key", "value");
    final StringFactory factory = mockSupplier();
    final String value = Keyed.getOrSet(this, map, "key", factory);
    assertEquals("value", value);
    verify(factory, never()).apply(eq("key"));
  }
  
  @Test
  public void testDoubleCheckedAbsent() {
    final Map<String, String> map = new HashMap<>();
    final StringFactory factory = mockSupplier();
    final String value = Keyed.getOrSetDoubleChecked(this, map, "key", factory);
    assertEquals("value", value);
    verify(factory).apply(eq("key"));
  }
  
  @Test
  public void testDoubleCheckedExisting() {
    final Map<String, String> map = new HashMap<>();
    map.put("key", "value");
    final StringFactory factory = mockSupplier();
    final String value = Keyed.getOrSetDoubleChecked(this, map, "key", factory);
    assertEquals("value", value);
    verify(factory, never()).apply(eq("key"));
  }
  
  @Test
  public void testForKey() {
    final StringFactory factory = mockSupplier();
    final Keyed<String, String> keyed = new Keyed<>(factory);
    
    final String value1 = keyed.forKey("key");
    assertEquals("value", value1);
    verify(factory).apply(eq("key"));
    reset(factory);
    
    final String value2 = keyed.forKey("key");
    assertEquals("value", value2);
    verify(factory, never()).apply(eq("key"));
    
    final Map<String, String> map = keyed.asMap();
    assertEquals(1, map.size());
    assertEquals("value", map.get("key"));
  }
  
  @Test
  public void testToString() {
    final Keyed<String, String> keyed = new Keyed<>(String::new);
    Assertions.assertToStringOverride(keyed);
  }
}
