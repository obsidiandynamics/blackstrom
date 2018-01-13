package com.obsidiandynamics.blackstrom.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

public abstract class FallibleMockProducer<K, V> extends MockProducer<K, V> {
  protected ExceptionGenerator<ProducerRecord<K, V>> sendExceptionGenerator = ExceptionGenerator.never();
  
  FallibleMockProducer(boolean autoComplete,
                       Serializer<K> keySerializer,
                       Serializer<V> valueSerializer) {
    super(autoComplete, keySerializer, valueSerializer);
  }
  
  public void setSendExceptionGenerator(ExceptionGenerator<ProducerRecord<K, V>> sendExceptionGenerator) {
    this.sendExceptionGenerator = sendExceptionGenerator;
  }
}
