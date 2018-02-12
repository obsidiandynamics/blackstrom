package com.obsidiandynamics.blackstrom.group;

import org.jgroups.*;

@FunctionalInterface
public interface HostMessageHandler {
  void handle(JChannel channel, Message message);
}