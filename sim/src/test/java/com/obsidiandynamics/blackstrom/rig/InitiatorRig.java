package com.obsidiandynamics.blackstrom.rig;

import static com.obsidiandynamics.blackstrom.model.AbortReason.*;
import static com.obsidiandynamics.blackstrom.model.Resolution.*;
import static java.util.concurrent.TimeUnit.*;
import static org.junit.Assert.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import org.jgroups.Message.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.bank.*;
import com.obsidiandynamics.blackstrom.factor.*;
import com.obsidiandynamics.blackstrom.group.*;
import com.obsidiandynamics.blackstrom.initiator.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.manifold.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.indigo.util.*;

public final class InitiatorRig {
  private static final int GROUP_VIEW_WAIT_MILLIS = 300_000;
  private static final int GROUP_ANNOUNCE_WAIT_MILLIS = 1_000;
  private static final int GROUP_ANNOUNCE_ATTEMPTS = 10;
  private static final int PROPOSAL_TIMEOUT_MILLIS = 30_000;
  private static final int BENCHMARK_FINALISE_MILLIS = PROPOSAL_TIMEOUT_MILLIS * 2;
  
  public static class Config extends RigConfig {
    int branches = 2;
    boolean guided = true;
    long runs;
    double warmupFraction = .1;
    double pAbort = 0.1;
    
    @Override void validate() {
      super.validate();
      assertNotEquals(0, runs);
    }
    
    public InitiatorRig create() { 
      return new InitiatorRig(this);
    }
  }
  
  private final Config config;
  
  private InitiatorRig(Config config) {
    config.validate();
    this.config = config;
  }
  
  public void run() throws Exception {
    final long transferAmount = 1;
    final long runs = config.runs;
    final int backlogTarget = (int) Math.min(runs / 10, 10_000);
    
    final AtomicLong commits = new AtomicLong();
    final AtomicLong aborts = new AtomicLong();
    final AtomicLong timeouts = new AtomicLong();

    final String sandboxKey = UUID.randomUUID().toString();
    try (Group group = new Group(config.channelFactory.get())) {
      config.log.info("Initiator: joining cluster '{}'", config.clusterName);
      group.connect(config.clusterName);
      final int groupSize = config.branches + (config.guided ? 2 : 1);
      config.log.info("Initiator: awaiting group formation ({} members required)", groupSize);
      Timesert.wait(GROUP_VIEW_WAIT_MILLIS).until(() -> assertEquals(groupSize, group.view().size()));
      for (int attempt = 0; attempt < GROUP_ANNOUNCE_ATTEMPTS; attempt++) {
        config.log.info("Initiator: announcing sandbox key {}", sandboxKey);
        final Future<?> f = group.gather(groupSize - 1,
                                         new AnnouncePacket(UUID.randomUUID(), sandboxKey), Flag.DONT_BUNDLE);
        try { 
          f.get(GROUP_ANNOUNCE_WAIT_MILLIS, MILLISECONDS);
          break;
        } catch (TimeoutException e) {
          config.log.warn("Initiator: timed out {}", sandboxKey);
          if (attempt == GROUP_ANNOUNCE_ATTEMPTS - 1) throw e;
        } finally {
          f.cancel(true);
        }
      }
      config.log.info("Initiator: warming up");
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
      final BankSettlement settlement0 = BankSettlement.forTwo(transferAmount);
      final BankSettlement settlement1 = BankSettlement.forTwo(-transferAmount);
      final BankSettlement settlementAbort = BankSettlement.forTwo(Integer.MAX_VALUE * 2L);
      final String[] branchIds = BankBranch.generateIds(2);
      
      final long warmupRuns = (long) (config.warmupFraction * runs);
      final int abortsPerMille = (int) (config.pAbort * 1000d);
      long startTime = 0;
      for (long run = 0; run < runs; run++) {
        if (run == warmupRuns) {
          config.log.info("Initiator: starting timed run");
          startTime = System.currentTimeMillis();
        }
        
        if (run % backlogTarget == 0) {
          long lastLogTime = 0;
          for (;;) {
            final int backlog = (int) (run - commits.get() - aborts.get() - timeouts.get());
            if (backlog > backlogTarget) {
              TestSupport.sleep(1);
              if (System.currentTimeMillis() - lastLogTime > 5_000) {
                config.log.debug(String.format("Initiator: throttling... backlog @ %,d (%,d txns)", backlog, run));
                lastLogTime = System.currentTimeMillis();
              }
            } else {
              break;
            }
          }
        }
        
        final BankSettlement settlement;
        if (run % 1000 < abortsPerMille) {
          settlement = settlementAbort;
        } else {
          settlement = run % 1 == 0 ? settlement0 : settlement1;
        }
        ledger.append(new Proposal(Long.toHexString(run), branchIds, settlement, PROPOSAL_TIMEOUT_MILLIS)
                      .withShardKey(sandbox.key()));
      }
      
      Timesert.wait(BENCHMARK_FINALISE_MILLIS).until(() -> {
        final long c = commits.get(), a = aborts.get(), t = timeouts.get();
        assertEquals(String.format("commits=%,d, aborts=%,d, timeouts=%,d", c, a, t), runs, c + a + t);
      });
      
      final long took = System.currentTimeMillis() - startTime;
      final long timedRuns = runs - warmupRuns;
      config.log.info(String.format("%,d took %,d ms, %,.0f txns/sec", 
                                    timedRuns, took, (double) timedRuns / took * 1000));
      config.log.info(String.format("%,d commits | %,d aborts | %,d timeouts", 
                                    commits.get(), aborts.get(), timeouts.get()));
    } finally {
      manifold.dispose();
    }
  }
}
