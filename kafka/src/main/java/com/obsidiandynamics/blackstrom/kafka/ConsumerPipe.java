package com.obsidiandynamics.blackstrom.kafka;

import java.util.concurrent.*;

import org.apache.kafka.clients.consumer.*;

import com.obsidiandynamics.blackstrom.kafka.KafkaReceiver.*;
import com.obsidiandynamics.blackstrom.worker.*;

public final class ConsumerPipe<K, V> implements Joinable {
  private final BlockingQueue<ConsumerRecords<K, V>> queue;
  
  private final RecordHandler<K, V> handler;
  
  private final WorkerThread thread;
  
  public ConsumerPipe(RecordHandler<K, V> handler, int backlogSize, String threadName) {
    this.handler = handler;
    queue = new LinkedBlockingQueue<>(backlogSize);
    thread = WorkerThread.builder()
        .withOptions(new WorkerOptions().withDaemon(true).withName(threadName))
        .onCycle(this::cycle)
        .buildAndStart();
  }
  
  public boolean receive(ConsumerRecords<K, V> records) {
    return queue.offer(records);
  }
  
  private void cycle(WorkerThread t) throws InterruptedException {
    for (;;) {
      final ConsumerRecords<K, V> records = queue.take();
      handler.onReceive(records);
    }
  }

  @Override
  public boolean join(long timeoutMillis) throws InterruptedException {
    return thread.join(timeoutMillis);
  }
  
  public Joinable terminate() {
    thread.terminate();
    return this;
  }
}
