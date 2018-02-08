package com.obsidiandynamics.blackstrom.ledger;

import java.util.concurrent.*;

import org.apache.kafka.clients.consumer.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.worker.*;

final class PipelinedConsumer implements Joinable {
  private final BlockingQueue<ConsumerRecords<String, Message>> queue;
  
  private final MessageContext context;
  
  private final MessageHandler handler;
  
  private final WorkerThread thread;
  
  PipelinedConsumer(MessageContext context, MessageHandler handler, int backlogSize, String threadName) {
    this.context = context;
    this.handler = handler;
    queue = new LinkedBlockingQueue<>(backlogSize);
    thread = WorkerThread.builder()
        .withOptions(new WorkerOptions().withDaemon(true).withName(threadName))
        .onCycle(this::cycle)
        .buildAndStart();
  }
  
  boolean enqueue(ConsumerRecords<String, Message> records) {
    return queue.offer(records);
  }
  
  private void cycle(WorkerThread t) throws InterruptedException {
    for (;;) {
      final ConsumerRecords<String, Message> records = queue.take();
      for (ConsumerRecord<String, Message> record : records) {
        final DefaultMessageId messageId = new DefaultMessageId(record.partition(), record.offset());
        final Message message = record.value();
        message.setMessageId(messageId);
        message.setShardKey(record.key());
        message.setShard(record.partition());
        handler.onMessage(context, message);
      }
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
