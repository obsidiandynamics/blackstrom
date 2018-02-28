package com.obsidiandynamics.blackstrom.model;

import org.apache.commons.lang3.builder.*;

import com.obsidiandynamics.blackstrom.util.*;

public abstract class Message {
  private static final int UNASSIGNED = -1;
  
  private final String ballotId;
  
  private final long timestamp;
  
  private MessageId messageId;
  
  private String source;
  
  private String shardKey;
  
  private int shard = UNASSIGNED;
  
  protected Message(String ballotId, long timestamp) {
    this.ballotId = ballotId;
    this.timestamp = timestamp != 0 ? timestamp : NanoClock.now();
  }
  
  public final String getBallotId() {
    return ballotId;
  }

  public final MessageId getMessageId() {
    return messageId;
  }
  
  public final void setMessageId(MessageId messageId) {
    this.messageId = messageId;
  }
  
  public final String getSource() {
    return source;
  }
  
  public final void setSource(String source) {
    this.source = source;
  }

  public final long getTimestamp() {
    return timestamp;
  }
  
  public final String getShardKey() {
    return shardKey;
  }
  
  public final void setShardKey(String shardKey) {
    this.shardKey = shardKey;
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
  
  public final void setShard(int shard) {
    this.shard = shard;
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
