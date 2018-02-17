package com.obsidiandynamics.blackstrom.rig;

import static com.obsidiandynamics.blackstrom.model.AbortReason.*;
import static com.obsidiandynamics.blackstrom.model.Resolution.*;
import static java.util.concurrent.TimeUnit.*;
import static org.junit.Assert.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;

import org.jgroups.*;
import org.slf4j.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.bank.*;
import com.obsidiandynamics.blackstrom.factor.*;
import com.obsidiandynamics.blackstrom.group.*;
import com.obsidiandynamics.blackstrom.initiator.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.manifold.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.blackstrom.util.throwing.*;
import com.obsidiandynamics.indigo.util.*;

public final class InitiatorRig {
  private static final Logger LOG = LoggerFactory.getLogger(InitiatorRig.class);
  
  private static final int GROUP_VIEW_WAIT_MILLIS = 300_000;
  private static final int GROUP_ANNOUNCE_WAIT_MILLIS = 10_000;
  private static final int PROPOSAL_TIMEOUT_MILLIS = 30_000;
  private static final int BENCHMARK_FINALISE_MILLIS = PROPOSAL_TIMEOUT_MILLIS * 2;
  
  public static class Config {
    Supplier<Ledger> ledgerFactory;
    CheckedSupplier<JChannel, Exception> channelFactory;
    String clusterName = "rig";
    int branches = 2;
    boolean guided = true;
    long runs;
    double warmupFraction = .1;
    double pAbort;
    
    void validate() {
      assertNotNull(ledgerFactory);
      assertNotNull(channelFactory);
      assertNotEquals(0, runs);
    }
  }
  
  private final Config config;
  
  public InitiatorRig(Config config) {
    config.validate();
    this.config = config;
  }
  
  public void run() throws Exception {
    final long transferAmount = 1;
    final long runs = config.runs;
    final int backlogTarget = (int) Math.min(runs / 10, 10_000);
    final double pAbort = config.pAbort;
    
    final AtomicLong commits = new AtomicLong();
    final AtomicLong aborts = new AtomicLong();
    final AtomicLong timeouts = new AtomicLong();

    final String sandboxKey = UUID.randomUUID().toString();
    try (Group group = new Group(config.channelFactory.get())) {
      LOG.info("Joining cluster '{}'", config.clusterName);
      group.connect(config.clusterName);
      final int groupSize = config.branches + (config.guided ? 2 : 1);
      LOG.info("Awaiting group formation ({} members required)", groupSize);
      Timesert.wait(GROUP_VIEW_WAIT_MILLIS).until(() -> assertEquals(groupSize, group.view().size()));
      LOG.info("Announcing sandbox key {}", sandboxKey);
      final Future<?> f = group.gather(new AnnouncePacket(sandboxKey, sandboxKey));
      try { 
        f.get(GROUP_ANNOUNCE_WAIT_MILLIS, MILLISECONDS);
      } finally {
        f.cancel(true);
      }
      LOG.info("Warming up");
    }
    
    final Sandbox sandbox = Sandbox.forKey(sandboxKey);
    final Initiator initiator = (NullGroupInitiator) (c, o) -> {
      if (sandbox.contains(o)) {
        (o.getResolution() == COMMIT ? commits : o.getAbortReason() == REJECT ? aborts : timeouts)
        .incrementAndGet();
      }
    };
    
    final Ledger ledger = config.ledgerFactory.get();
    final Manifold manifold = Manifold.builder()
        .withLedger(ledger)
        .withFactors(initiator)
        .build();
    
    try {
      final BankSettlement sA = BankSettlement.forTwo(transferAmount);
      final BankSettlement sB = BankSettlement.forTwo(-transferAmount);
      final String[] branchIds = BankBranch.generateIds(2);
      
      final long warmupRunPairs = (long) (config.warmupFraction * runs / 2d);
      long startTime = 0;
      for (long run = 0, runPair = 0; runPair < runs / 2; runPair++) {
        if (runPair == warmupRunPairs) {
          LOG.info("Starting timed run");
          startTime = System.currentTimeMillis();
        }
        
        if (run % backlogTarget == 0) {
          long lastLogTime = 0;
          for (;;) {
            final int backlog = (int) (run - commits.get() - aborts.get() - timeouts.get());
            if (backlog > backlogTarget) {
              TestSupport.sleep(1);
              if (System.currentTimeMillis() - lastLogTime > 5_000) {
                LOG.debug(String.format("Throttling... backlog @ %,d (%,d txns)", backlog, run));
                lastLogTime = System.currentTimeMillis();
              }
            } else {
              break;
            }
          }
        }
        
        ledger.append(new Proposal(Long.toHexString(run++), branchIds, sA, PROPOSAL_TIMEOUT_MILLIS)
                      .withShardKey(sandbox.key()));
        ledger.append(new Proposal(Long.toHexString(run++), branchIds, sB, PROPOSAL_TIMEOUT_MILLIS)
                      .withShardKey(sandbox.key()));
      }
      
      Timesert.wait(BENCHMARK_FINALISE_MILLIS).until(() -> {
        final long c = commits.get(), a = aborts.get(), t = timeouts.get();
        assertEquals(String.format("commits=%,d, aborts=%,d, timeouts=%,d", c, a, t), runs, c + a + t);
      });
      
      final long took = System.currentTimeMillis() - startTime;
      final long timedRuns = runs - warmupRunPairs * 2;
      LOG.info(String.format("%,d took %,d ms, %,.0f txns/sec (%,d commits | %,d aborts | %,d timeouts)", 
                             timedRuns, took, (double) timedRuns / took * 1000, commits.get(), aborts.get(), timeouts.get()));
    } finally {
      manifold.dispose();
    }
  }
}
