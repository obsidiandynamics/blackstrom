package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;

/**
 *  A fail-safe, best effort resolver of consumer group offsets. 
 */
@FunctionalInterface
public interface ConsumerGroupOffsetsResolver {
  /**
   *  Resolves the offsets for the given consumer group and collection of topic-partitions.<p>
   *  
   *  The resolver must not fail; all errors must be trapped internally. In the event of an error,
   *  the resolver must return an empty map.
   *  
   *  @param groupId The group ID.
   *  @param topicPartitions The topic-partition pairs.
   *  @return The offsets {@link Map}.
   */
  Map<TopicPartition, OffsetAndMetadata> resolve(String groupId, Collection<TopicPartition> topicPartitions);
}
