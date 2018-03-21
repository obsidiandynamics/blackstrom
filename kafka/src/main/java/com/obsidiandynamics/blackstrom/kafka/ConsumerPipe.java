package com.obsidiandynamics.blackstrom.kafka;

import java.util.concurrent.*;

import org.apache.kafka.clients.consumer.*;

import com.obsidiandynamics.blackstrom.kafka.KafkaReceiver.*;
import com.obsidiandynamics.blackstrom.worker.*;

public final class ConsumerPipe<K, V> implements Terminable, Joinable {
  private final BlockingQueue<ConsumerRecords<K, V>> queue;
  
  private final RecordHandler<K, V> handler;
  
  private final WorkerThread thread;
  
  public ConsumerPipe(ConsumerPipeConfig config, RecordHandler<K, V> handler, String threadName) {
    this.handler = handler;
    queue = new LinkedBlockingQueue<>(config.getBacklogBatches());
    if (config.isAsync()) {
      thread = WorkerThread.builder()
          .withOptions(new WorkerOptions().daemon().withName(threadName))
          .onCycle(this::cycle)
          .buildAndStart();
    } else {
      thread = null;
    }
  }
  
  public boolean receive(ConsumerRecords<K, V> records) throws InterruptedException {
    if (thread != null) {
      return queue.offer(records);
    } else {
      handler.onReceive(records);
      return true;
    }
  }
  
  private void cycle(WorkerThread t) throws InterruptedException {
    for (;;) {
      final ConsumerRecords<K, V> records = queue.take();
      handler.onReceive(records);
    }
  }

  @Override
  public boolean join(long timeoutMillis) throws InterruptedException {
    return thread != null ? thread.join(timeoutMillis) : true;
  }
  
  @Override
  public Joinable terminate() {
    if (thread != null) thread.terminate();
    return this;
  }
}
