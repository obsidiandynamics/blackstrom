package com.obsidiandynamics.blackstrom;

import com.obsidiandynamics.blackstrom.machine.*;

public interface Initable {
  void init(VotingMachine machine);
  
  interface Default extends Initable {
    @Override
    default void init(VotingMachine machine) {}
  }
}
