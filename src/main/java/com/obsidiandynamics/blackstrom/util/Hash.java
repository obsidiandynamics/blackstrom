package com.obsidiandynamics.blackstrom.util;

import com.obsidiandynamics.blackstrom.model.*;

public final class Hash {
  private Hash() {}
  
  public static int getShard(Message message, int shards) {
    if (message.isShardAssigned()) {
      final int shard = message.getShard();
      if (shard >= shards) {
        throw new IndexOutOfBoundsException("The supplied shard index " + shard + " is outside of permissible bouds 0-" + (shards - 1));
      }
      return shard;
    } else {
      final Object shardKey = message.getShardKey();
      if (shardKey != null) {
        return fold(shardKey.hashCode()) % shards;
      } else {
        return 0;
      }
    }
  }
  
  public static int fold(int hash) {
    return hash < 0 ? hash - Integer.MIN_VALUE : hash;
  }
}
