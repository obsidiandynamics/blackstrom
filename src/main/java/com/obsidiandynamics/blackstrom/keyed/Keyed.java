package com.obsidiandynamics.blackstrom.keyed;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

/**
 *  An atomic map with a {@link Keyed#forKey(Object)} value lookup that atomically creates and
 *  assigns a value if one does not already exist, using a factory supplied in the constructor.<p>
 *  
 *  This class is useful for creating a map of shards, where a shard can be anything, but is typically a 
 *  list, queue or another map.<p>
 *  
 *  It can also be used for write-through and write-back caching, where it is imperative that at most
 *  one value is mapped to a given key at any point in time.
 *  
 *  @param <K> The key type.
 *  @param <V> The value type.
 */
public class Keyed<K, V> {
  protected final Map<K, V> map;
  
  protected final Function<K, V> valueFactory;

  public Keyed(Function<K, V> valueFactory) {
    this(new ConcurrentHashMap<>(), valueFactory);
  }
  
  public Keyed(Map<K, V> map, Function<K, V> valueFactory) {
    this.map = map;
    this.valueFactory = valueFactory;
  }

  /**
   *  Returns the backing map, wrapped using {@link Collections#unmodifiableMap(Map)}.
   *  
   *  @return The backing map.
   */
  public final Map<K, V> asMap() {
    return Collections.unmodifiableMap(map);
  }
  
  /**
   *  Looks up a value for the key, creating one if it doesn't already exist.
   *  
   *  @param key The key.
   *  @return The value (existing or created).
   */
  public final V forKey(K key) {
    return getOrSetDoubleChecked(map, map, key, valueFactory);
  }
  
  /**
   *  Utility for atomically retrieving a mapped value if one exists, or assigning a value from a 
   *  given factory if it doesn't.<p>
   *  
   *  This variant uses the double-checked locking pattern, internally delegating to
   *  {@link Keyed#getOrSet(Object, Map, Object, Function)}.
   *  
   *  @param <K> Key type.
   *  @param <V> Value type.
   *  @param lock The lock object to use.
   *  @param map The map.
   *  @param key The key.
   *  @param valueFactory A way of creating a missing value.
   *  @return The value (existing or created).
   */
  public static <K, V> V getOrSetDoubleChecked(Object lock, Map<K, V> map, K key, Function<K, V> valueFactory) {
    final V existing = map.get(key);
    if (existing != null) {
      return existing;
    } else {
      return getOrSet(lock, map, key, valueFactory);
    }
  }
  
  public static <K, V> V getOrSet(Object lock, Map<K, V> map, K key, Function<K, V> valueFactory) {
    synchronized (lock) {
      final V existing = map.get(key);
      if (existing != null) {
        return existing;
      } else {
        final V created = valueFactory.apply(key);
        map.put(key, created);
        return created;
      }
    }
  }

  @Override
  public final String toString() {
    return getClass().getSimpleName() + " [map=" + map + "]";
  }
}
