package com.obsidiandynamics.blackstrom.rig;

import static org.junit.Assert.*;

import java.util.function.*;

import org.jgroups.*;
import org.slf4j.*;

import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.util.throwing.*;

public abstract class RigConfig {
  Logger log;
  Supplier<Ledger> ledgerFactory;
  CheckedSupplier<JChannel, Exception> channelFactory;
  String clusterName = "rig";
  
  void validate() {
    assertNotNull(log);
    assertNotNull(ledgerFactory);
    assertNotNull(channelFactory);
  }
}
