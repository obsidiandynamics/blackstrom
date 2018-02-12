package com.obsidiandynamics.blackstrom.group;

import java.util.*;

import org.jgroups.*;

@FunctionalInterface
public interface GroupMessageHandler {
  void handle(JChannel channel, Map<Address, Message> messages);
}