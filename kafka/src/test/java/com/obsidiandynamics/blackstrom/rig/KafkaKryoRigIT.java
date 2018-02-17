package com.obsidiandynamics.blackstrom.rig;

import com.obsidiandynamics.blackstrom.group.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.rig.InitiatorRig.*;

public final class KafkaKryoRigIT {
  public static final class Initiator {
    public static void main(String[] args) throws Exception {
      new InitiatorRig(new Config() {{
        ledgerFactory = MultiNodeQueueLedger::new;
        channelFactory = Group::newLoopbackChannel;
        runs = 1000;
      }}).run();
    }
  }
}
