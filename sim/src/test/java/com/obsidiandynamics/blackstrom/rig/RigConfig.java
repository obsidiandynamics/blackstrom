package com.obsidiandynamics.blackstrom.rig;

import static org.junit.Assert.*;

import java.util.function.*;

import org.jgroups.*;

import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.func.*;
import com.obsidiandynamics.zerolog.*;

public abstract class RigConfig {
  Zlg zlg = Zlg.forDeclaringClass().get();
  Supplier<Ledger> ledgerFactory;
  CheckedSupplier<JChannel, Exception> channelFactory;
  String clusterName = "rig";
  
  void validate() {
    assertNotNull(ledgerFactory);
    assertNotNull(channelFactory);
  }
}
