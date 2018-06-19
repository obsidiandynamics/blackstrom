package com.obsidiandynamics.blackstrom.model;

import org.apache.commons.lang3.builder.*;

import com.obsidiandynamics.nanoclock.*;

public abstract class Message implements Cloneable {
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
  
  protected final int baseHashCode() {
    return new HashCodeBuilder()
        .append(ballotId)
        .append(timestamp)
        .append(messageId)
        .append(source)
        .append(shardKey)
        .append(shard)
        .toHashCode();
  }
  
  protected final boolean baseEquals(Message that) {
    return new EqualsBuilder()
        .append(ballotId, that.ballotId)
        .append(timestamp, that.timestamp)
        .append(messageId, that.messageId)
        .append(source, that.source)
        .append(shardKey, that.shardKey)
        .append(shard, that.shard)
        .isEquals();
  }
  
  protected final String baseToString() {
    return "ballotId=" + ballotId + ", messageId=" + messageId + ", source=" + source + ", shardKey=" + shardKey + 
        ", shard=" + shard + ", timestamp=" + timestamp;
  }
  
  @Override
  public abstract Message clone();
  
  protected static <M extends Message> M copyMutableFields(M original, M clone) {
    final Message originalMessage = original;
    final Message cloneMessage = clone;
    cloneMessage.messageId = originalMessage.messageId;
    cloneMessage.source = originalMessage.source;
    cloneMessage.shardKey = originalMessage.shardKey;
    cloneMessage.shard = originalMessage.shard;
    return clone;
  }
}
