package com.obsidiandynamics.blackstrom.kafka;

import org.apache.kafka.clients.producer.*;
import org.slf4j.*;

import com.obsidiandynamics.blackstrom.nodequeue.*;
import com.obsidiandynamics.blackstrom.worker.*;

public final class ProducerPipe<K, V> implements Joinable {
  private static final int QUEUE_BACKOFF_MILLIS = 1;
  
  private static class AsyncRecord<K, V> {
    final ProducerRecord<K, V> record;
    final Callback callback;
    
    AsyncRecord(ProducerRecord<K, V> record, Callback callback) {
      this.record = record;
      this.callback = callback;
    }
  }
  
  private final NodeQueue<AsyncRecord<K, V>> queue = new NodeQueue<>();
  
  private final QueueConsumer<AsyncRecord<K, V>> queueConsumer = queue.consumer();
  
  private final Producer<K, V> producer;
  
  private final WorkerThread thread;
  
  private final Logger log;
  
  private volatile boolean producerDisposed;
  
  public ProducerPipe(Producer<K, V> producer, String threadName, Logger log) {
    this.producer = producer;
    this.log = log;
    thread = WorkerThread.builder()
        .withOptions(new WorkerOptions().withDaemon(true).withName(threadName))
        .onCycle(this::cycle)
        .buildAndStart();
  }
  
  public void send(ProducerRecord<K, V> record, Callback callback) {
    queue.add(new AsyncRecord<>(record, callback));
  }
  
  private void cycle(WorkerThread t) throws InterruptedException {
    final AsyncRecord<K, V> rec = queueConsumer.poll();
    if (rec != null) {
      try {
        producer.send(rec.record, rec.callback);
      } catch (Throwable e) {
        if (! producerDisposed) {
          log.error(String.format("Error sending %s", rec.record), e);
        }
      }
    } else {
      Thread.sleep(QUEUE_BACKOFF_MILLIS);
    }
  }
  
  public Joinable terminate() {
    thread.terminate();
    closeProducer();
    return this;
  }
  
  void closeProducer() {
    producerDisposed = true;
    producer.close();
  }

  @Override
  public boolean join(long timeoutMillis) throws InterruptedException {
    return thread.join(timeoutMillis);
  }
}
