package com.obsidiandynamics.blackstrom.util;

import java.util.*;

import com.obsidiandynamics.blackstrom.model.*;

public final class Shard {
  private final String key;
  
  private Shard(String key) {
    this.key = key;
  }
  
  public String key() {
    return key;
  }
  
  public boolean contains(Message message) {
    return Objects.equals(key, message.getShardKey());
  }
  
  public static Shard forTest(Object test) {
    final String key = test.getClass().getSimpleName() + "-" + UUID.randomUUID();
    return new Shard(key);
  }
  
  @Override
  public String toString() {
    return Shard.class.getSimpleName() + " [key=" + key + "]";
  }
}
