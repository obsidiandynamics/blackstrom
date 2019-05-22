package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;

//TODO is this needed?
public final class NopOffsetsResolver implements OffsetsResolver {
  private static final NopOffsetsResolver INSTANCE = new NopOffsetsResolver();
  
  public static NopOffsetsResolver getInstance() { return INSTANCE; }
  
  private NopOffsetsResolver() {}

  @Override
  public Map<TopicPartition, OffsetAndMetadata> resolve(String groupId) {
    return Collections.emptyMap();
  }
}
