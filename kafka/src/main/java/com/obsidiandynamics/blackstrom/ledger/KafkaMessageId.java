package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;

import org.apache.commons.lang3.builder.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;

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
    if (this == obj) {
      return true;
    } else if (obj instanceof KafkaMessageId) {
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
  
  public Map<TopicPartition, OffsetAndMetadata> toOffset() {
    return Collections.singletonMap(new TopicPartition(topic, partition), new OffsetAndMetadata(offset));
  }

  public static KafkaMessageId fromRecord(ConsumerRecord<?, ?> record) {
    return new KafkaMessageId(record.topic(), record.partition(), record.offset());
  }
}
