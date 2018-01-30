package com.obsidiandynamics.blackstrom.util;

import java.util.*;

import com.obsidiandynamics.blackstrom.model.*;

public final class Sandbox {
  private final String key;
  
  private Sandbox(String key) {
    this.key = key;
  }
  
  public String key() {
    return key;
  }
  
  public boolean contains(Message message) {
    return Objects.equals(key, message.getShardKey());
  }
  
  public static Sandbox forTest(Object test) {
    final String key = test.getClass().getSimpleName() + "-" + UUID.randomUUID();
    return new Sandbox(key);
  }
  
  @Override
  public String toString() {
    return Sandbox.class.getSimpleName() + " [key=" + key + "]";
  }
}
