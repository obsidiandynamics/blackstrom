package com.obsidiandynamics.blackstrom.ledger;

import static com.obsidiandynamics.func.Functions.*;

import java.util.*;
import java.util.stream.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;

/**
 *  A tuple representing a Kafka partition and an offset within that partition.
 */
final class PartitionOffset {
  private final int partition;
  private final long offset;
  
  public PartitionOffset(int partition, long offset) {
    this.partition = partition;
    this.offset = offset;
  }
  
  public int getPartition() {
    return partition;
  }
  
  public long getOffset() {
    return offset;
  }
  
  @Override
  public int hashCode() {
    final var prime = 31;
    var result = 1;
    result = prime * result + partition;
    result = prime * result + Long.hashCode(offset);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj instanceof PartitionOffset) {
      final var that = (PartitionOffset) obj;
      return partition == that.partition && offset == that.offset;
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return partition + "#" + offset;
  }
  
  static List<PartitionOffset> summarise(Map<TopicPartition, OffsetAndMetadata> partitionOffsets) {
    mustExist(partitionOffsets, "Partition offsets must be specified");
    return partitionOffsets.entrySet().stream()
        .map(partitionOffset -> new PartitionOffset(partitionOffset.getKey().partition(), partitionOffset.getValue().offset()))
        .sorted(byField(PartitionOffset::getPartition, Integer::compare)).collect(Collectors.toList());
  }
}
