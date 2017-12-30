package com.obsidiandynamics.blackstrom.handler;

import com.obsidiandynamics.blackstrom.*;

public interface TypedMessageHandler extends Disposable.Default {
  default void init(InitContext context) {}
  
  default MessageHandler toUntypedHandler() {
    return new MessageHandlerAdapter(this);
  }
}
