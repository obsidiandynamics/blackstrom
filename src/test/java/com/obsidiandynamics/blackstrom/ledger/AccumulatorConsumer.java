package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;

import com.obsidiandynamics.blackstrom.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.worker.*;

final class AccumulatorConsumer implements Disposable {
  private final Accumulator accumulator;
  private final WorkerThread thread;
  private final MessageHandler handler;
  private final MessageContext context;
  
  AccumulatorConsumer(Accumulator accumulator, MessageHandler handler) {
    this.accumulator = accumulator;
    this.handler = handler;
    thread = WorkerThread.builder()
        .withOptions(new WorkerOptions().withDaemon(true))
        .onCycle(this::cycle)
        .buildAndStart();
    final Object handlerId = this;
    context = new DefaultMessageContext(null, handlerId);
  }
  
  private final List<Message> sink = new ArrayList<>();
  private long nextOffset;
  
  private void cycle(WorkerThread t) throws InterruptedException {
    accumulator.retrieve(nextOffset, sink);
    if (! sink.isEmpty()) {
      for (Message m : sink) {
        handler.onMessage(context, m);
      }
      nextOffset = ((ShardMessageId) sink.get(sink.size() - 1).getMessageId()).getOffset() + 1;
      sink.clear();
    } else {
      Thread.sleep(1);
    }
  }
  
  @Override
  public void dispose() {
    thread.terminate();
  }
}
