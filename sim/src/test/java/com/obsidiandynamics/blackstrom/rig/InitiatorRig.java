package com.obsidiandynamics.blackstrom.rig;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;

import org.jgroups.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.factor.*;
import com.obsidiandynamics.blackstrom.group.*;
import com.obsidiandynamics.blackstrom.initiator.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.manifold.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.blackstrom.util.throwing.*;

public final class InitiatorRig {
  private final int GROUP_VIEW_WAIT_MILLIS = 300_000;
  private final int GROUP_ANNOUNCE_WAIT_MILLIS = 10_000;
  
  public static final class Config {
    Supplier<Ledger> ledgerFactory = MultiNodeQueueLedger::new;
    CheckedSupplier<JChannel, Exception> channelFactory = Group::newLoopbackChannel;
    int branches = 2;
    boolean guided = true;
    long runs;
    double pAbort;
  }
  
  private final Config config;
  
  public InitiatorRig(Config config) {
    this.config = config;
  }
  
  public void run() throws Exception {
    final long transferAmount = 1_000;
    final long runs = config.runs;
    final int backlogTarget = (int) Math.min(runs / 10, 10_000);
    final double pAbort = config.pAbort;
    
    final AtomicLong commits = new AtomicLong();
    final AtomicLong aborts = new AtomicLong();
    final AtomicLong timeouts = new AtomicLong();

    final String sandboxKey = UUID.randomUUID().toString();
    try (Group group = new Group(config.channelFactory.get())) {
      final int groupSize = config.branches + (config.guided ? 2 : 1);
      Await.boundedTimeout(GROUP_VIEW_WAIT_MILLIS, () -> group.view().size() == groupSize);
      final Future<Map<Address, org.jgroups.Message>> f = group.gather(new AnnouncePacket(sandboxKey, sandboxKey));
      try { 
        f.get(GROUP_ANNOUNCE_WAIT_MILLIS, TimeUnit.MILLISECONDS);
      } finally {
        f.cancel(true);
      }
    }
    
    final Sandbox sandbox = Sandbox.forKey(sandboxKey);
    final Initiator initiator = (NullGroupInitiator) (c, o) -> {
      if (sandbox.contains(o)) {
        (o.getResolution() == Resolution.COMMIT ? commits : o.getAbortReason() == AbortReason.REJECT ? aborts : timeouts)
        .incrementAndGet();
      }
    };
    
    final Ledger ledger = config.ledgerFactory.get();
    final Manifold manifold = Manifold.builder()
        .withLedger(ledger)
        .withFactors(initiator)
        .build();
    
    
    
    manifold.dispose();
  }
}
