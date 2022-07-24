package com.obsidiandynamics.blackstrom.rig;

import static com.obsidiandynamics.blackstrom.model.AbortReason.*;
import static com.obsidiandynamics.blackstrom.model.Resolution.*;
import static java.util.concurrent.TimeUnit.*;
import static org.junit.Assert.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import org.HdrHistogram.*;
import org.jgroups.Message.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.bank.*;
import com.obsidiandynamics.blackstrom.factor.*;
import com.obsidiandynamics.blackstrom.initiator.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.manifold.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.monitor.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.jgroups.*;
import com.obsidiandynamics.nanoclock.*;
import com.obsidiandynamics.threads.*;
import com.obsidiandynamics.worker.*;

public final class InitiatorRig {
  private static final int GROUP_VIEW_WAIT_MILLIS = 300_000;
  private static final int GROUP_ANNOUNCE_ATTEMPTS = 10;
  private static final int PROPOSAL_TIMEOUT_MILLIS = 30_000;
  private static final int BENCHMARK_FINALISE_MILLIS = PROPOSAL_TIMEOUT_MILLIS * 2;
  
  public static class Config extends RigConfig {
    int branches = 2;
    boolean guided = true;
    long runs;
    double warmupFraction = .1;
    double pAbort = 0.1;
    int backlogTarget = 10_000;
    boolean histogram;
    int groupAnnounceWaitMillis = 5_000;
    int progressIntervalMillis = 2_000;
    
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
    final int backlogTarget = (int) Math.max(1, Math.min(runs / 10, config.backlogTarget));

    final String sandboxKey = UUID.randomUUID().toString();
    try (Group group = new Group(config.channelFactory.get())) {
      config.zlg.i("Initiator: joining cluster '%s'", z -> z.arg(config.clusterName));
      group.connect(config.clusterName);
      final int groupSize = config.branches + (config.guided ? 2 : 1);
      config.zlg.i("Initiator: awaiting group formation (%d members required)", z -> z.arg(groupSize));
      Timesert.wait(GROUP_VIEW_WAIT_MILLIS).until(() -> assertEquals(groupSize, group.view().size()));
      for (int attempt = 0; attempt < GROUP_ANNOUNCE_ATTEMPTS; attempt++) {
        config.zlg.i("Initiator: announcing sandbox key %s", z -> z.arg(sandboxKey));
        final Future<?> f = group.gather(groupSize - 1,
                                         new AnnouncePacket(UUID.randomUUID(), sandboxKey), Flag.DONT_BUNDLE);
        try { 
          f.get(config.groupAnnounceWaitMillis, MILLISECONDS);
          break;
        } catch (TimeoutException e) {
          config.zlg.w("Initiator: timed out %s", z -> z.arg(sandboxKey));
          if (attempt == GROUP_ANNOUNCE_ATTEMPTS - 1) throw e;
        } finally {
          f.cancel(true);
        }
      }
      config.zlg.i("Initiator: warming up");
    }

    final boolean histogram = config.histogram;
    final Histogram hist = histogram ? new Histogram(NANOSECONDS.toNanos(10), SECONDS.toNanos(10), 5) : null;
    final AtomicLong commits = new AtomicLong();
    final AtomicLong aborts = new AtomicLong();
    final AtomicLong timeouts = new AtomicLong();
    
    final AtomicBoolean timedRunStarted = new AtomicBoolean();
    final AtomicLong startTime = new AtomicLong();
    final long warmupRuns = (long) (config.warmupFraction * runs);
    final WorkerThread progressMonitorThread = WorkerThread.builder()
        .withOptions(new WorkerOptions().daemon().withName(InitiatorRig.class, "progress"))
        .onCycle(__thread -> {
          Thread.sleep(config.progressIntervalMillis);
          if (timedRunStarted.get()) {
            final long c = commits.get(), a = aborts.get(), t = timeouts.get(), s = c + a + t;
            final long took = System.currentTimeMillis() - startTime.get();
            final long timedRuns = Math.max(0, s - warmupRuns);
            final double rate = 1000d * timedRuns / took;
            config.zlg.i("%,d commits | %,d aborts | %,d timeouts | %,d total [%,.0f/s]", 
                         z -> z.arg(c).arg(a).arg(t).arg(s).arg(rate));
          }
        })
        .buildAndStart();
    
    final Sandbox sandbox = Sandbox.forKey(sandboxKey);
    final Initiator initiator = (NullGroupChoreograpyInitiator) (c, o) -> {
      if (sandbox.contains(o)) {
        if (histogram && timedRunStarted.get()) {
          final OutcomeMetadata meta = o.getMetadata();
          final long latency = NanoClock.now() - meta.getProposalTimestamp();
          hist.recordValue(latency);
        }
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
      
      final int abortsPerMille = (int) (config.pAbort * 1000d);
      for (long run = 0; run < runs; run++) {
        if (run == warmupRuns) {
          config.zlg.i("Initiator: starting timed run");
          startTime.set(System.currentTimeMillis());
          timedRunStarted.set(true);
        }
        
        if (run % backlogTarget == 0) {
          long lastLogTime = 0;
          for (;;) {
            final int backlog = (int) (run - commits.get() - aborts.get() - timeouts.get());
            if (backlog >= backlogTarget) {
              Threads.sleep(1);
              if (System.currentTimeMillis() - lastLogTime > 5_000) {
                final long _run = run;
                config.zlg.d("Initiator: throttling... backlog @ %,d (%,d txns)", z -> z.arg(backlog).arg(_run));
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
          settlement = run % 2 == 0 ? settlement0 : settlement1;
        }
        ledger.append(new Proposal(Long.toHexString(run), NanoClock.now(), branchIds, settlement, 
                                   PROPOSAL_TIMEOUT_MILLIS)
                      .withShardKey(sandbox.key()));
      }
      progressMonitorThread.terminate().joinSilently();
      
      Timesert.wait(BENCHMARK_FINALISE_MILLIS).until(() -> {
        final long c = commits.get(), a = aborts.get(), t = timeouts.get();
        assertTrue(String.format("commits=%,d, aborts=%,d, timeouts=%,d", c, a, t), c + a + t >= runs);
      });
      
      final long took = System.currentTimeMillis() - startTime.get();
      final long timedRuns = runs - warmupRuns;
      final double rate = (double) timedRuns / took * 1000;
      config.zlg.i("%,d took %,d ms, %,.0f txns/sec", z -> z.arg(timedRuns).arg(took).arg(rate));
      final long c = commits.get(), a = aborts.get(), t = timeouts.get();
      config.zlg.i("%,d commits | %,d aborts | %,d timeouts | %,d total", 
                   z -> z.arg(c).arg(a).arg(t).arg(c + a + t));
      
      if (histogram) {
        final long min = hist.getMinValue();
        final double mean = hist.getMean();
        final long p50 = hist.getValueAtPercentile(50.0);
        final long p95 = hist.getValueAtPercentile(95.0);
        final long p99 = hist.getValueAtPercentile(99.0);
        final long max = hist.getMaxValue();
        config.zlg.i("min: %,d, mean: %,.0f, 50%%: %,d, 95%%: %,d, 99%%: %,d, max: %,d (ns)", 
                     z -> z.arg(min).arg(mean).arg(p50).arg(p95).arg(p99).arg(max));
      }
    } finally {
      manifold.dispose();
    }
  }
}
