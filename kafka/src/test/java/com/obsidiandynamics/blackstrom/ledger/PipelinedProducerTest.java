package com.obsidiandynamics.blackstrom.ledger;

import static org.mockito.Mockito.*;

import java.util.*;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;
import org.junit.*;
import org.slf4j.*;

import com.obsidiandynamics.blackstrom.kafka.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.indigo.util.*;

public final class PipelinedProducerTest {
  @Test
  public void testSendDisposed() {
    final Logger log = mock(Logger.class);
    final Kafka<String, Message> kafka = new MockKafka<>();
    final Properties props = new PropertiesBuilder()
        .with("key.serializer", StringSerializer.class.getName())
        .with("value.serializer", KafkaMessageSerializer.class.getName())
        .build();
    final Producer<String, Message> producer = kafka.getProducer(props);
    final PipelinedProducer<String, Message> pp = 
        new PipelinedProducer<>(producer, PipelinedProducer.class.getSimpleName(), log);

    try {
      pp.closeProducer();
      final Proposal proposal = new Proposal("B100", new String[0], null, 0);
      final ProducerRecord<String, Message> rec = new  ProducerRecord<>("test", proposal);
      pp.enqueue(rec, null);
      
      TestSupport.sleep(10);
      verifyNoMoreInteractions(log);
    } finally {
      pp.terminate().joinQuietly();
    }
  }
}
