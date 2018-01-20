package com.obsidiandynamics.blackstrom.ledger;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.Message;
import com.obsidiandynamics.indigo.*;

/**
 *  
 */
public final class IndigoLedger implements Ledger {
  private static final String DISPATCH_ROLE = "dispatch";
  private static final String SUBSCRIBER_ROLE = "subscriber";
  

  private final ActorRef dispatchRef = ActorRef.of(DISPATCH_ROLE);
  
  private final ActorSystem system;
  private final MessageContext context = new DefaultMessageContext(this, null);
  
  public IndigoLedger() {
    system = new ActorSystemConfig(){{
      defaultActorConfig = new ActorConfig() {{
        bias = 1000;
        backlogThrottleCapacity = Long.MAX_VALUE;
      }}; 
    }}.createActorSystem()
        .on(DISPATCH_ROLE).cue(DispatchActor::new)
        .on(SUBSCRIBER_ROLE).cue(SubscriberActor::new);
  }
  
  private static class DispatchActor implements Actor {
    private int numHandlers;
    
    @Override
    public void act(Activation a, com.obsidiandynamics.indigo.Message m) {
      if (m.body() instanceof MessageHandler) {
        a.to(ActorRef.of(SUBSCRIBER_ROLE, String.valueOf(numHandlers++))).tell(m.body());
      } else {
        for (int handlerId = 0; handlerId < numHandlers; handlerId++) {
          a.to(ActorRef.of(SUBSCRIBER_ROLE, String.valueOf(handlerId))).tell(m.body());
        }
      }
    }
  }
  
  private class SubscriberActor implements Actor {
    private MessageHandler handler;
    
    @Override
    public void act(Activation a, com.obsidiandynamics.indigo.Message m) {
      if (m.body() instanceof MessageHandler) {
        handler = m.body();
      } else {
        handler.onMessage(context, m.body());
      }
    }
  }
  
  @Override
  public void attach(MessageHandler handler) {
    system.tell(dispatchRef, handler);
  }

  @Override
  public void append(Message message, AppendCallback callback) {
    system.tell(dispatchRef, message);
    callback.onAppend(message.getMessageId(), null);
  }
  
  @Override
  public void confirm(Object handlerId, Object messageId) {}

  @Override
  public void dispose() {
    system.shutdownQuietly();
  }
}
