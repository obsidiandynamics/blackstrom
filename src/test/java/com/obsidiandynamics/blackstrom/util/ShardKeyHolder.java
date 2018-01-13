package com.obsidiandynamics.blackstrom.util;

import java.util.*;

import com.obsidiandynamics.blackstrom.model.*;

public final class ShardKeyHolder {
  private final String key;
  
  private ShardKeyHolder(String key) {
    this.key = key;
  }
  
  public String key() {
    return key;
  }
  
  public boolean produced(Message message) {
    return Objects.equals(key, message.getShardKey());
  }
  
  public static ShardKeyHolder forTest(Object test) {
    final String key = test.getClass().getSimpleName() + "%" + Long.toHexString(System.currentTimeMillis());
    return new ShardKeyHolder(key);
  }
  
  @Override
  public String toString() {
    return ShardKeyHolder.class.getSimpleName() + "[key=" + key + "]";
  }
}
