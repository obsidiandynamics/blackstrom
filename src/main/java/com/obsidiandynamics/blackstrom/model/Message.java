package com.obsidiandynamics.blackstrom.model;

import org.apache.commons.lang3.builder.*;

public abstract class Message {
  private static final int UNASSIGNED = -1;
  
  private final Object ballotId;
  
  private final long timestamp;
  
  private Object messageId;
  
  private String source;
  
  private String shardKey;
  
  private int shard = UNASSIGNED;
  
  protected Message(Object ballotId, long timestamp) {
    this.ballotId = ballotId;
    this.timestamp = timestamp != 0 ? timestamp : System.currentTimeMillis();
  }
  
  public final Object getBallotId() {
    return ballotId;
  }

  public final Object getMessageId() {
    return messageId;
  }
  
  public final Message withMessageId(Object messageId) {
    this.messageId = messageId;
    return this;
  }

  public final String getSource() {
    return source;
  }
  
  public final Message withSource(String source) {
    this.source = source;
    return this;
  }

  public final long getTimestamp() {
    return timestamp;
  }
  
  public final String getShardKey() {
    return shardKey;
  }
  
  public final Message withShardKey(String shardKey) {
    this.shardKey = shardKey;
    return this;
  }
  
  public final int getShard() {
    return shard;
  }
  
  public final boolean isShardAssigned() {
    return shard != UNASSIGNED;
  }
  
  public final Integer getShardIfAssigned() {
    return shard != UNASSIGNED ? shard : null;
  }
  
  public final Message withShard(int shard) {
    this.shard = shard;
    return this;
  }
  
  public abstract MessageType getMessageType();  
  
  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(ballotId)
        .append(timestamp)
        .append(messageId)
        .append(source)
        .append(shardKey)
        .append(shard)
        .toHashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Message) {
      final Message that = (Message) obj;
      return new EqualsBuilder()
          .append(ballotId, that.ballotId)
          .append(timestamp, that.timestamp)
          .append(messageId, that.messageId)
          .append(source, that.source)
          .append(shardKey, that.shardKey)
          .append(shard, that.shard)
          .isEquals();
    } else {
      return false;
    }
  }
  
  protected final String baseToString() {
    return "ballotId=" + ballotId + ", messageId=" + messageId + ", source=" + source + ", shardKey=" + shardKey + 
        ", shard=" + shard + ", timestamp=" + timestamp;
  }
}
