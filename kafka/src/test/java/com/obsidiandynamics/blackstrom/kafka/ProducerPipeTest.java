package com.obsidiandynamics.blackstrom.kafka;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.junit.*;
import org.slf4j.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.indigo.util.*;

public final class ProducerPipeTest {
  private final Timesert wait = Wait.SHORT;
  
  private ProducerPipe<String, Message> pipe;
  
  @After
  public void after() {
    if (pipe != null) pipe.terminate().joinSilently();
  }
  
  @Test
  public void testSendDisposedAsync() {
    final Logger log = mock(Logger.class);
    final Kafka<String, Message> kafka = new MockKafka<>();
    final Producer<String, Message> producer = kafka.getProducer(TestProps.producer());
    pipe = new ProducerPipe<>(new ProducerPipeConfig().withAsync(true), producer, ProducerPipe.class.getSimpleName(), log);

    pipe.closeProducer();
    final Proposal proposal = new Proposal("B100", new String[0], null, 0);
    final ProducerRecord<String, Message> rec = new  ProducerRecord<>("test", proposal);
    pipe.send(rec, null);
    
    TestSupport.sleep(10);
    verifyNoMoreInteractions(log);
  }
  
  @Test
  public void testSendAsync() {
    final Logger log = mock(Logger.class);
    final Kafka<String, Message> kafka = new MockKafka<>();
    final Producer<String, Message> producer = kafka.getProducer(TestProps.producer());
    pipe = new ProducerPipe<>(new ProducerPipeConfig().withAsync(true), producer, ProducerPipe.class.getSimpleName(), log);

    final Consumer<String, Message> consumer = kafka.getConsumer(TestProps.consumer());
    consumer.subscribe(Arrays.asList("test"));
    final Proposal proposal = new Proposal("B100", new String[0], null, 0);
    final ProducerRecord<String, Message> rec = new  ProducerRecord<>("test", proposal);
    pipe.send(rec, null);
    
    wait.until(() -> {
      assertEquals(1, consumer.poll(1).count());
    });
  }
  
  @Test
  public void testSendSync() {
    final Logger log = mock(Logger.class);
    final Kafka<String, Message> kafka = new MockKafka<>();
    final Producer<String, Message> producer = kafka.getProducer(TestProps.producer());
    pipe = new ProducerPipe<>(new ProducerPipeConfig().withAsync(false), producer, ProducerPipe.class.getSimpleName(), log);

    final Consumer<String, Message> consumer = kafka.getConsumer(TestProps.consumer());
    consumer.subscribe(Arrays.asList("test"));
    final Proposal proposal = new Proposal("B100", new String[0], null, 0);
    final ProducerRecord<String, Message> rec = new  ProducerRecord<>("test", proposal);
    pipe.send(rec, null);
    
    wait.until(() -> {
      assertEquals(1, consumer.poll(1).count());
    });
  }
  
  @Test
  public void testSendError() {
    final Logger log = mock(Logger.class);
    final Kafka<String, Message> kafka = new MockKafka<String, Message>()
        .withSendRuntimeExceptionGenerator(r -> new RuntimeException("testSendError"));
    final Producer<String, Message> producer = kafka.getProducer(TestProps.producer());
    pipe = new ProducerPipe<>(new ProducerPipeConfig().withAsync(false), producer, ProducerPipe.class.getSimpleName(), log);

    final Proposal proposal = new Proposal("B100", new String[0], null, 0);
    final ProducerRecord<String, Message> rec = new  ProducerRecord<>("test", proposal);
    pipe.send(rec, null);
    
    wait.until(() -> {
      verify(log).error(any(), (Throwable) any());
    });
  }
}
