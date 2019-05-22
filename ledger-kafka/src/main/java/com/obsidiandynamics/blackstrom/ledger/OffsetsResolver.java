package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;

@FunctionalInterface
public interface OffsetsResolver {
  Map<TopicPartition, OffsetAndMetadata> resolve(String groupId);
}
