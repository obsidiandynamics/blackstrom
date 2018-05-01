package com.obsidiandynamics.blackstrom.rig;

import java.util.*;
import java.util.function.*;

import org.jgroups.*;
import org.junit.*;
import org.junit.runner.*;
import org.junit.runners.*;

import com.obsidiandynamics.blackstrom.*;
import com.obsidiandynamics.blackstrom.bank.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.func.*;
import com.obsidiandynamics.jgroups.*;
import com.obsidiandynamics.junit.*;
import com.obsidiandynamics.testmark.*;
import com.obsidiandynamics.zerolog.*;

@RunWith(Parameterized.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public final class MultiNodeQueueRigTest {
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return TestCycle.timesQuietly(1);
  }
  
  private final int scale = Testmark.getOptions(Scale.class, Scale.unity()).magnitude();
  
  private final List<Disposable> cleanup = new ArrayList<>();
  
  @After
  public void after() {
    cleanup.forEach(d -> d.dispose());
    cleanup.clear();
  }
  
  @Test
  public void test() throws Exception {
    test(1_000, 2, false);
  }
  
  @Test
  public void testBenchmarkLatency() throws Exception {
    Testmark.ifEnabled("latency", () -> {
      test(1_000 * scale, 2, true);
    });
  }
  
  @Test
  public void testBenchmarkThroughput() throws Exception {
    Testmark.ifEnabled("throughput", () -> {
      test(4_000_000 * scale, 2, false);
    });
  }
  
  private void test(long runs, int branches, boolean lowLatency) throws Exception {
    final CheckedSupplier<JChannel, Exception> _channelFactory = Group::newLoopbackChannel;
    final int maxYields = lowLatency ? Integer.MAX_VALUE : 100;
    final MultiNodeQueueLedger ledger = new MultiNodeQueueLedger(new MultiNodeQueueLedger.Config()
                                                                 .withMaxYields(maxYields));
    final Supplier<Ledger> _ledgerFactory = () -> ledger;
    final Zlg _zlg = Zlg.forDeclaringClass().get();
    final long _runs = runs;

    final InitiatorRig initiator = new InitiatorRig.Config() {{
      zlg = _zlg;
      ledgerFactory = _ledgerFactory;
      channelFactory = _channelFactory;
      runs = _runs;
      backlogTarget = lowLatency ? 1 : 10_000;
      histogram = lowLatency;
    }}.create();
    
    final String[] branchIds = BankBranch.generateIds(branches);
    for (String _branchId : branchIds) {
      final CohortRig cohort = new CohortRig.Config() {{
        zlg = _zlg;
        ledgerFactory = _ledgerFactory;
        channelFactory = _channelFactory;
        branchId = _branchId;
      }}.create();
      cleanup.add(cohort);
    }
    
    final MonitorRig monitor = new MonitorRig.Config() {{
      zlg = _zlg;
      ledgerFactory = _ledgerFactory;
      channelFactory = _channelFactory;
      metadataEnabled = true;
    }}.create();
    cleanup.add(monitor);
    
    initiator.run();
  }
  
  public static void main(String[] args) {
    Testmark.enable().withOptions(Scale.by(8));
    JUnitCore.runClasses(MultiNodeQueueRigTest.class);
  }
}