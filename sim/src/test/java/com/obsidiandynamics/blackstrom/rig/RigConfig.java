package com.obsidiandynamics.blackstrom.rig;

import static org.junit.Assert.*;

import java.util.function.*;

import org.jgroups.*;
import org.slf4j.*;

import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.func.*;

public abstract class RigConfig {
  Logger log = LoggerFactory.getLogger(RigConfig.class);
  Supplier<Ledger> ledgerFactory;
  CheckedSupplier<JChannel, Exception> channelFactory;
  String clusterName = "rig";
  
  void validate() {
    assertNotNull(ledgerFactory);
    assertNotNull(channelFactory);
  }
}
