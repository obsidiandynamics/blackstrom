package com.obsidiandynamics.blackstrom.ledger;

import org.apache.kafka.clients.producer.*;

import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.nodequeue.*;
import com.obsidiandynamics.blackstrom.worker.*;

final class PipelinedProducer implements Joinable {
  private static final int QUEUE_BACKOFF_MILLIS = 1;
  
  private static class AsyncRecord {
    final ProducerRecord<String, Message> record;
    final Callback callback;
    
    AsyncRecord(ProducerRecord<String, Message> record, Callback callback) {
      this.record = record;
      this.callback = callback;
    }
  }
  
  private final NodeQueue<AsyncRecord> queue = new NodeQueue<>();
  
  private final QueueConsumer<AsyncRecord> queueConsumer = queue.consumer();
  
  private final Producer<String, Message> producer;
  
  private final WorkerThread thread;
  
  PipelinedProducer(Producer<String, Message> producer, String threadName) {
    this.producer = producer;
    thread = WorkerThread.builder()
        .withOptions(new WorkerOptions().withDaemon(true).withName(threadName))
        .onCycle(this::cycle)
        .buildAndStart();
  }
  
  void enqueue(ProducerRecord<String, Message> record, Callback callback) {
    queue.add(new AsyncRecord(record, callback));
  }
  
  private void cycle(WorkerThread t) throws InterruptedException {
    final AsyncRecord rec = queueConsumer.poll();
    if (rec != null) {
      producer.send(rec.record, rec.callback);
    } else {
      Thread.sleep(QUEUE_BACKOFF_MILLIS);
    }
  }

  @Override
  public boolean join(long timeoutMillis) throws InterruptedException {
    return thread.join(timeoutMillis);
  }
  
  Joinable terminate() {
    thread.terminate();
    return this;
  }
}
