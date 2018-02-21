package com.obsidiandynamics.blackstrom.group;

import org.jgroups.*;

@FunctionalInterface
public interface ChannelFactory {
  JChannel create() throws Exception;
}