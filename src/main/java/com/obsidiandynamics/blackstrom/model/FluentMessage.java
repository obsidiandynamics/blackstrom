package com.obsidiandynamics.blackstrom.model;

import com.obsidiandynamics.func.*;

public abstract class FluentMessage<M extends FluentMessage<?>> extends Message {
  protected FluentMessage(String xid, long timestamp) {
    super(xid, timestamp);
  }
  
  public final M withMessageId(MessageId messageId) {
    setMessageId(messageId);
    return self();
  }

  public final M withSource(String source) {
    setSource(source);
    return self();
  }

  public final M withShardKey(String shardKey) {
    setShardKey(shardKey);
    return self();
  }
  
  public final M withShard(int shard) {
    setShard(shard);
    return self();
  }
  
  public final M inResponseTo(Message origin) {
    respondTo(origin);
    return self();
  }
  
  private M self() {
    return Classes.cast(this);
  }
}
