package com.obsidiandynamics.blackstrom.rig;

import java.util.*;
import java.util.function.*;

import org.jgroups.*;
import org.junit.*;
import org.junit.runner.*;
import org.junit.runners.*;
import org.slf4j.*;

import com.obsidiandynamics.blackstrom.*;
import com.obsidiandynamics.blackstrom.bank.*;
import com.obsidiandynamics.blackstrom.group.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.blackstrom.util.throwing.*;
import com.obsidiandynamics.junit.*;

@RunWith(Parameterized.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public final class MultiNodeQueueRigTest {
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return TestCycle.timesQuietly(1);
  }
  
  private final int SCALE = Testmark.getOptions(Scale.class, Scale.UNITY).magnitude();
  
  private final List<Disposable> cleanup = new ArrayList<>();
  
  @After
  public void after() {
    cleanup.forEach(d -> d.dispose());
    cleanup.clear();
  }
  
  @Test
  public void test() throws Exception {
    test(1_000, 2);
  }
  
  @Test
  public void testBenchmark() throws Exception {
    Testmark.ifEnabled(() -> test(4_000_000 * SCALE, 2));
  }
  
  private void test(long runs, int branches) throws Exception {
    final CheckedSupplier<JChannel, Exception> _channelFactory = Group::newLoopbackChannel;
    final MultiNodeQueueLedger ledger = new MultiNodeQueueLedger();
    final Supplier<Ledger> _ledgerFactory = () -> ledger;
    final Logger _log = LoggerFactory.getLogger(MultiNodeQueueRigTest.class);
    final long _runs = runs;

    final InitiatorRig initiator = new InitiatorRig.Config() {{
      log = _log;
      ledgerFactory = _ledgerFactory;
      channelFactory = _channelFactory;
      runs = _runs;
    }}.create();
    
    final String[] branchIds = BankBranch.generateIds(branches);
    for (String _branchId : branchIds) {
      final CohortRig cohort = new CohortRig.Config() {{
        log = _log;
        ledgerFactory = _ledgerFactory;
        channelFactory = _channelFactory;
        branchId = _branchId;
      }}.create();
      cleanup.add(cohort);
    }
    
    final MonitorRig monitor = new MonitorRig.Config() {{
      log = _log;
      ledgerFactory = _ledgerFactory;
      channelFactory = _channelFactory;
    }}.create();
    cleanup.add(monitor);
    
    initiator.run();
  }
  
  public static void main(String[] args) {
    Testmark.enable().withOptions(Scale.by(4));
    JUnitCore.runClasses(MultiNodeQueueRigTest.class);
  }
}