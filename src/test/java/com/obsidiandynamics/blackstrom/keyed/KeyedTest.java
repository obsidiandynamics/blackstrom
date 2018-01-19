package com.obsidiandynamics.blackstrom.keyed;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.*;
import java.util.function.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;

public final class KeyedTest {
  private interface StringSupplier extends Supplier<String> {}
  
  private static StringSupplier mockSupplier() {
    final StringSupplier factory = mock(StringSupplier.class);
    when(factory.get()).thenReturn("value");
    return factory;
  }
  
  @Test
  public void testSingleCheckedAbsent() {
    final Map<String, String> map = new HashMap<>();
    final StringSupplier factory = mockSupplier();
    final String value = Keyed.getOrSetSingleChecked(this, map, "key", factory);
    assertEquals("value", value);
    verify(factory).get();
  }
  
  @Test
  public void testSingleCheckedExisting() {
    final Map<String, String> map = new HashMap<>();
    map.put("key", "value");
    final StringSupplier factory = mockSupplier();
    final String value = Keyed.getOrSetSingleChecked(this, map, "key", factory);
    assertEquals("value", value);
    verify(factory, never()).get();
  }
  
  @Test
  public void testDoubleCheckedAbsent() {
    final Map<String, String> map = new HashMap<>();
    final StringSupplier factory = mockSupplier();
    final String value = Keyed.getOrSetDoubleChecked(this, map, "key", factory);
    assertEquals("value", value);
    verify(factory).get();
  }
  
  @Test
  public void testDoubleCheckedExisting() {
    final Map<String, String> map = new HashMap<>();
    map.put("key", "value");
    final StringSupplier factory = mockSupplier();
    final String value = Keyed.getOrSetDoubleChecked(this, map, "key", factory);
    assertEquals("value", value);
    verify(factory, never()).get();
  }
  
  @Test
  public void testForKey() {
    final StringSupplier factory = mockSupplier();
    final Keyed<String, String> keyed = new Keyed<>(factory);
    
    final String value1 = keyed.forKey("key");
    assertEquals("value", value1);
    verify(factory).get();
    reset(factory);
    
    final String value2 = keyed.forKey("key");
    assertEquals("value", value2);
    verify(factory, never()).get();
    
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
