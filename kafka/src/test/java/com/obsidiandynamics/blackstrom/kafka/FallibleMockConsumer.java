package com.obsidiandynamics.blackstrom.kafka;

import java.util.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;

public abstract class FallibleMockConsumer<K, V> extends MockConsumer<K, V> {
  protected ExceptionGenerator<Map<TopicPartition, OffsetAndMetadata>> commitExceptionGenerator = ExceptionGenerator.never();
  
  FallibleMockConsumer(OffsetResetStrategy offsetResetStrategy) {
    super(offsetResetStrategy);
  }
  
  public void setCommitExceptionGenerator(ExceptionGenerator<Map<TopicPartition, OffsetAndMetadata>> commitExceptionGenerator) {
    this.commitExceptionGenerator = commitExceptionGenerator;
  }
}
