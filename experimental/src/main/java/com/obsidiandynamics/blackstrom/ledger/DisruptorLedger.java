package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;
import java.util.concurrent.*;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;

/**
 *  
 */
public final class DisruptorLedger implements Ledger {
  private final MessageContext context = new DefaultMessageContext(this);
  
  private static class MessageEvent {
    private Message message;

    Message get() {
      return message;
    }

    void set(Message message) {
      this.message = message;
    }
    
    void clear() {
      message = null;
    }
  }
  
  private final Disruptor<MessageEvent> disruptor;
  
  private final List<EventHandler<MessageEvent>> handlers = new ArrayList<>();
  
  public DisruptorLedger(int ringBufferSize) {
    final EventFactory<MessageEvent> eventFactory = MessageEvent::new;
    final ThreadFactory threadFactory = Thread::new;
    disruptor = new Disruptor<>(eventFactory, ringBufferSize, threadFactory);
  }
  
  @Override
  public void attach(MessageHandler handler) {
    handlers.add((e, seq, endOfBatch) -> {
      handler.onMessage(context, e.get());
    });
  }

  @Override
  public void append(Message message) throws Exception {
    final RingBuffer<MessageEvent> buf = disruptor.getRingBuffer();
    final long sequence = buf.next();
    try {
      final MessageEvent event = buf.get(sequence);
      event.set(message);
    } finally {
      buf.publish(sequence);
    }
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public void init() {
    final EventHandler<MessageEvent> collector = (e, seq, endOfBatch) -> e.clear();
    
    disruptor
    .handleEventsWith(handlers.toArray(new EventHandler[handlers.size()]))
    .then(collector);
    
    handlers.clear();
    disruptor.start();
  }

  @Override
  public void dispose() {
    disruptor.shutdown();
  }
}
