package com.obsidiandynamics.blackstrom.ledger;

import org.apache.commons.lang3.builder.*;

public final class KafkaMessageId {
  private final String topic;
  
  private final int partition;
  
  private final long offset;

  public KafkaMessageId(String topic, int partition, long offset) {
    this.topic = topic;
    this.partition = partition;
    this.offset = offset;
  }
  
  String getTopic() {
    return topic;
  }

  int getPartition() {
    return partition;
  }

  long getOffset() {
    return offset;
  }
  
  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(topic)
        .append(partition)
        .append(offset)
        .toHashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof KafkaMessageId) {
      final KafkaMessageId other = (KafkaMessageId) obj;
      return new EqualsBuilder()
          .append(topic, other.topic)
          .append(partition, other.partition)
          .append(offset, other.offset)
          .isEquals();
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return topic + "-" + partition + "@" + offset;
  }
}
