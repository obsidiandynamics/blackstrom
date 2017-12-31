package com.obsidiandynamics.blackstrom.factor;

import java.util.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.machine.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.scheduler.*;

public final class FailureProneFactor implements Factor, NominationProcessor, VoteProcessor, OutcomeProcessor {
  private final Factor backingFactor;
  
  private Ledger backingLedger;
  
  private final MessageHandler backingHandler;
  
  private final EnumMap<FailureType, FailureModes> failureModes = new EnumMap<>(FailureType.class);
  
  private final TaskScheduler scheduler = new TaskScheduler();
  
  public FailureProneFactor(Factor backingFactor) {
    this.backingFactor = backingFactor;
    backingHandler = new MessageHandlerAdapter(backingFactor);
  }
  
  public FailureProneFactor withFailureModes(FailureModes... failureModes) {
    for (FailureModes failureMode : failureModes) {
      this.failureModes.put(failureMode.getFailureType(), failureMode);
    }
    return this;
  }

  @Override
  public void init(InitContext context) {
    backingLedger = context.getLedger();
    backingFactor.init(new InitContext() {
      @Override public Ledger getLedger() {
        return new Ledger() {
          @Override public void attach(MessageHandler handler) {
            throw new UnsupportedOperationException();
          }

          @Override public void append(Message message) throws Exception {
            onSend(message);
          }
        };
      }
    });
    scheduler.start();
  }
 
  @Override
  public void dispose() {
    scheduler.terminate();
    scheduler.joinQuietly();
    backingFactor.dispose();
  }

  @Override
  public void onNomination(MessageContext context, Nomination nomination) {
    onReceive(context, nomination);
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
    for (FailureModes failureMode : failureModes.values()) {
      switch (failureMode.getFailureType()) {
        case RX_DUPLICATE:
          onRxDuplicate(failureMode, context, message);
          break;
          
        case RX_DELAY:
          onRxDelay(failureMode, context, message);
          break;
          
        default:
      }
    }
  }
  
  private void onRxDuplicate(FailureModes failureMode, MessageContext context, Message message) {
    if (! failureMode.isTime()) return;
    
    backingHandler.onMessage(context, message);
    backingHandler.onMessage(context, message);
  }
  
  private void onRxDelay(FailureModes failureMode, MessageContext context, Message message) {
    if (! failureMode.isTime()) return;    
    
    final DelayedFailure delayed = failureMode.getExtent();
    runLater(message.getTimestamp() + delayed.getDelayMillis(), message.getBallotId(), () -> {
      backingHandler.onMessage(context, message);
    });
  }
  
  private void onSend(Message message) throws Exception {
    for (FailureModes failureMode : failureModes.values()) {
      switch (failureMode.getFailureType()) {
        case TX_DUPLICATE:
          onTxDuplicate(failureMode, message);
          break;
          
        case TX_DELAY:
          onTxDelay(failureMode, message);
          break;
          
        default:
      }
    }
  }
  
  private void onTxDuplicate(FailureModes failureMode, Message message) throws Exception {
    if (! failureMode.isTime()) return;
    
    backingLedger.append(message);
    backingLedger.append(message);
  }

  private void onTxDelay(FailureModes failureMode, Message message) {
    if (! failureMode.isTime()) return;
    
    final DelayedFailure delayed = failureMode.getExtent();
    runLater(message.getTimestamp() + delayed.getDelayMillis(), message.getBallotId(), () -> {
      try {
        backingLedger.append(message);
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
  }
  
  private void runLater(long time, Object ballotId, Runnable task) {
    scheduler.schedule(new Task() {
      @Override
      public long getTime() {
        return time;
      }

      @Override
      public Comparable<?> getId() {
        return (Comparable<?>) ballotId;
      }

      @Override
      public void execute(TaskScheduler scheduler) {
        task.run();
      }
    });
  }
}
