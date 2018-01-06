package com.obsidiandynamics.blackstrom.handler;

import com.obsidiandynamics.blackstrom.model.*;

public interface MessageHandler extends Groupable {
  void onMessage(MessageContext context, Message message);
}
