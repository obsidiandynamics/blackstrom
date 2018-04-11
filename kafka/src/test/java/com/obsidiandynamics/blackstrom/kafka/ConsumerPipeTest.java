package com.obsidiandynamics.blackstrom.kafka;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;
import org.junit.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.kafka.KafkaReceiver.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.func.*;

public final class ConsumerPipeTest {
  private final Timesert wait = Wait.SHORT;
  
  private ConsumerPipe<String, Message> pipe;
  
  @After
  public void after() {
    if (pipe != null) pipe.terminate().joinSilently();
  }
  
  private static ConsumerRecords<String, Message> records(ConsumerRecord<String, Message> record) {
    return new ConsumerRecords<>(Collections.singletonMap(new TopicPartition(record.topic(), record.partition()), 
                                                          Collections.singletonList(record)));
  }
  
  @Test
  public void testReceiveAsync() throws InterruptedException {
    final RecordHandler<String, Message> handler = Classes.cast(mock(RecordHandler.class));
    pipe = new ConsumerPipe<>(new ConsumerPipeConfig().withAsync(true), handler, ConsumerPipe.class.getSimpleName());
    
    final Proposal proposal = new Proposal("B100", new String[0], null, 0);
    final ConsumerRecords<String, Message> records = records(new ConsumerRecord<>("test", 0, 0, "key", proposal));
    pipe.receive(records);
    
    wait.until(() -> {
      try {
        verify(handler).onReceive(eq(records));
      } catch (InterruptedException e) {
        throw new AssertionError("Unexpected exception", e);
      }
    });
  }
  
  @Test
  public void testReceiveSync() throws InterruptedException {
    final RecordHandler<String, Message> handler = Classes.cast(mock(RecordHandler.class));
    pipe = new ConsumerPipe<>(new ConsumerPipeConfig().withAsync(false), handler, ConsumerPipe.class.getSimpleName());
    
    final Proposal proposal = new Proposal("B100", new String[0], null, 0);
    final ConsumerRecords<String, Message> records = records(new ConsumerRecord<>("test", 0, 0, "key", proposal));
    pipe.receive(records);
    
    verify(handler).onReceive(eq(records));
  }
}
