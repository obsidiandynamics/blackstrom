package com.obsidiandynamics.blackstrom.factor;

import java.util.function.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.retention.*;
import com.obsidiandynamics.scheduler.*;

public final class FallibleFactor implements Factor, ProposalProcessor, VoteProcessor, OutcomeProcessor {
  private final Factor backingFactor;
  
  private Ledger backingLedger;
  
  private final MessageHandler backingHandler;
  
  private FailureMode rxFailureMode;
  
  private FailureMode txFailureMode;
  
  private final TaskScheduler scheduler = new TaskScheduler();
  
  private final Object backingHandlerLock = new Object();
  
  private final Ledger interceptedLedger = new Ledger() {
    @Override 
    public Object attach(MessageHandler handler) {
      throw new UnsupportedOperationException();
    }

    @Override 
    public void append(Message message, AppendCallback callback) {
      onSend(message, callback);
    }
    
    @Override
    public void confirm(Object handlerId, MessageId messageId) {
      backingLedger.confirm(handlerId, messageId);
    }

    @Override
    public boolean isAssigned(Object handlerId, int shard) {
      return backingLedger.isAssigned(handlerId, shard);
    }
  };
  
  public FallibleFactor(Factor backingFactor) {
    this.backingFactor = backingFactor;
    backingHandler = new MessageHandlerAdapter(backingFactor);
  }
  
  public FallibleFactor withRxFailureMode(FailureMode rxFailureMode) {
    this.rxFailureMode = rxFailureMode;
    return this;
  }

  public FallibleFactor withTxFailureMode(FailureMode txFailureMode) {
    this.txFailureMode = txFailureMode;
    return this;
  }
  
  @Override
  public void init(InitContext context) {
    backingLedger = context.getLedger();
    backingFactor.init(new InitContext() {
      @Override 
      public Ledger getLedger() {
        return interceptedLedger;
      }
    });
    scheduler.start();
  }
 
  @Override
  public void dispose() {
    scheduler.terminate().joinSilently();
    backingFactor.dispose();
  }

  @Override
  public void onProposal(MessageContext context, Proposal proposal) {
    onReceive(context, proposal);
  }

  @Override
  public void onVote(MessageContext context, Vote vote) {
    onReceive(context, vote);
  }

  @Override
  public void onOutcome(MessageContext context, Outcome outcome) {
    onReceive(context, outcome);
  }
  
  private void onReceive(MessageContext context, Message message) {
    final MessageContext intercepedContext = new MessageContext() {
      @Override
      public Ledger getLedger() {
        return interceptedLedger;
      }

      @Override
      public Object getHandlerId() {
        return context.getHandlerId();
      }

      @Override
      public Retention getRetention() {
        return context.getRetention();
      }
    };
    
    if (rxFailureMode != null && rxFailureMode.isTime()) {
      switch (rxFailureMode.getFailureType()) {
        case DUPLICATE_DELIVERY:
          onRxDuplicate(intercepedContext, message);
          break;
          
        case DELAYED_DELIVERY:
          onRxDelayed((DelayedDelivery) rxFailureMode, intercepedContext, message);
          break;
          
        case DELAYED_DUPLICATE_DELIVERY:
          onRxDelayedDuplicate((DelayedDuplicateDelivery) rxFailureMode, intercepedContext, message);
          break;
          
        case $UNKNOWN:
        default:
          throw new UnsupportedOperationException("Unsupported failure mode " + rxFailureMode.getFailureType());
      }
    } else {
      forwardToHandler(intercepedContext, message);
    }
  }
  
  private void forwardToHandler(MessageContext context, Message message) {
    synchronized (backingHandlerLock) {
      backingHandler.onMessage(context, message);
    }
  }
  
  private void onRxDuplicate(MessageContext context, Message message) {
    forwardToHandler(context, message);
    forwardToHandler(context, message);
  }
  
  private void onRxDelayed(DelayedDelivery mode, MessageContext context, Message message) {
    runLater(mode.getDelayMillis(), message.getXid(), t -> {
      forwardToHandler(context, message);
    });
  }
  
  private void onRxDelayedDuplicate(DelayedDuplicateDelivery mode, MessageContext context, Message message) {
    forwardToHandler(context, message);
    runLater(mode.getDelayMillis(), message.getXid(), t -> {
      forwardToHandler(context, message);
    });
  }
  
  private void onSend(Message message, AppendCallback callback) {
    if (txFailureMode != null && txFailureMode.isTime()) {
      switch (txFailureMode.getFailureType()) {
        case DUPLICATE_DELIVERY:
          onTxDuplicate(message, callback);
          break;
          
        case DELAYED_DELIVERY:
          onTxDelayed((DelayedDelivery) txFailureMode, message, callback);
          break;
          
        case DELAYED_DUPLICATE_DELIVERY:
          onTxDelayedDuplicate((DelayedDuplicateDelivery) txFailureMode, message, callback);
          break;

        case $UNKNOWN:
        default:
          throw new UnsupportedOperationException("Unsupported failure mode " + txFailureMode.getFailureType());
      }
    } else {
      backingLedger.append(message, callback);
    }
  }
  
  private void onTxDuplicate(Message message, AppendCallback callback) {
    // because a message is mutable, we can't use it twice for sending; instead we copy it before attempting duplicate delivery
    final Message copy = message.shallowCopy();
    backingLedger.append(message, callback);
    backingLedger.append(copy, callback);
  }
  
  private void onTxDelayed(DelayedDelivery mode, Message message, AppendCallback callback) {
    runLater(mode.getDelayMillis(), message.getXid(), t -> {
      backingLedger.append(message, callback);
    });
  }
  
  private void onTxDelayedDuplicate(DelayedDuplicateDelivery mode, Message message, AppendCallback callback) {
    // because a message is mutable, we can't use it twice for sending; instead we copy it before attempting duplicate delivery
    final Message copy = message.shallowCopy();
    
    backingLedger.append(message, callback);
    runLater(mode.getDelayMillis(), copy.getXid(), t -> {
      backingLedger.append(copy, callback);
    });
  }
  
  private Task runLater(long delayMillis, Object xid, Consumer<Task> job) {
    final long time = System.nanoTime() + delayMillis * 1_000_000L;
    final Task task = new Task() {
      @Override
      public long getTime() {
        return time;
      }

      @Override
      public Comparable<?> getId() {
        return (Comparable<?>) xid;
      }

      @Override
      public void execute(TaskScheduler scheduler) {
        job.accept(this);
      }
    };
    scheduler.schedule(task);
    return task;
  }

  @Override
  public String getGroupId() {
    return backingHandler.getGroupId();
  }
}
