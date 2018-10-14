package com.obsidiandynamics.blackstrom.handler;

import com.obsidiandynamics.blackstrom.model.*;

@FunctionalInterface
public interface MessageTarget {
  void onMessage(MessageContext context, Message message);
}
