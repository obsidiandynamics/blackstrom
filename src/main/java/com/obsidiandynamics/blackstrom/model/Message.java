package com.obsidiandynamics.blackstrom.model;

import java.util.*;

import org.apache.commons.lang3.builder.*;

import com.obsidiandynamics.nanoclock.*;

public abstract class Message {
  public static final long NOW = 0;
  
  private static final int UNASSIGNED = -1;
  
  private final String xid;
  
  private final long timestamp;
  
  private MessageId messageId;
  
  private String source;
  
  private String shardKey;
  
  private int shard = UNASSIGNED;
  
  protected Message(String xid, long timestamp) {
    this.xid = xid;
    this.timestamp = timestamp != NOW ? timestamp : NanoClock.now();
  }
  
  public final String getXid() {
    return xid;
  }

  public final MessageId getMessageId() {
    return messageId;
  }
  
  public final void setMessageId(MessageId messageId) {
    if (this.messageId != null && ! Objects.equals(this.messageId, messageId)) {
      throw new IllegalArgumentException("Message ID cannot be reassigned (current: " + this.messageId + ", new: " + messageId + ")");
    }
    this.messageId = messageId;
  }
  
  public final String getSource() {
    return source;
  }
  
  public final void setSource(String source) {
    if (this.source != null && ! Objects.equals(this.source, source)) {
      throw new IllegalArgumentException("Source cannot be reassigned (current: " + this.source + ", new: " + source + ")");
    }
    this.source = source;
  }

  public final long getTimestamp() {
    return timestamp;
  }
  
  public final String getShardKey() {
    return shardKey;
  }
  
  public final void setShardKey(String shardKey) {
    if (this.shardKey != null && ! Objects.equals(this.shardKey, shardKey)) {
      throw new IllegalArgumentException("Shard key cannot be reassigned (current: " + this.shardKey + ", new: " + shardKey + ")");
    }
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
    if (this.shard != UNASSIGNED && this.shard != shard) {
      throw new IllegalArgumentException("Shard cannot be reassigned (current: " + this.shard + ", new: " + shard + ")");
    }
    this.shard = shard;
  }
  
  public abstract MessageType getMessageType();  
  
  public final void respondTo(Message origin) {
    setShardKey(origin.getShardKey());
    setShard(origin.getShard());
  }
  
  protected final int baseHashCode() {
    return new HashCodeBuilder()
        .append(xid)
        .append(timestamp)
        .append(messageId)
        .append(source)
        .append(shardKey)
        .append(shard)
        .toHashCode();
  }
  
  protected final boolean baseEquals(Message that) {
    return new EqualsBuilder()
        .append(xid, that.xid)
        .append(timestamp, that.timestamp)
        .append(messageId, that.messageId)
        .append(source, that.source)
        .append(shardKey, that.shardKey)
        .append(shard, that.shard)
        .isEquals();
  }
  
  protected final String baseToString() {
    return "xid=" + xid + ", messageId=" + messageId + ", source=" + source + ", shardKey=" + shardKey + 
        ", shard=" + shard + ", timestamp=" + timestamp;
  }
  
  public abstract Message shallowCopy();
  
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
