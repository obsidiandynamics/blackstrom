package com.obsidiandynamics.blackstrom.rig;

import java.util.*;
import java.util.function.*;

import org.jgroups.*;
import org.junit.*;

import com.obsidiandynamics.blackstrom.*;
import com.obsidiandynamics.blackstrom.bank.*;
import com.obsidiandynamics.blackstrom.group.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.util.throwing.*;

public final class MultiNodeRigTest {
  private final List<Disposable> rigs = new ArrayList<>();
  
  @After
  public void after() {
    rigs.forEach(d -> d.dispose());
    rigs.clear();
  }
  
  @Test
  public void test() throws Exception {
    test(1000, 2);
  }
  
  private void test(long runs, int branches) throws Exception {
    final CheckedSupplier<JChannel, Exception> globalChannelFactory = Group::newLoopbackChannel;
    final MultiNodeQueueLedger ledger = new MultiNodeQueueLedger();
    final Supplier<Ledger> globalLedgerFactory = () -> ledger;
    
    final long _runs = runs;
    final InitiatorRig initiator = new InitiatorRig(new InitiatorRig.Config() {{
      ledgerFactory = globalLedgerFactory;
      channelFactory = globalChannelFactory;
      runs = _runs;
    }});
    
    final String[] branchIds = BankBranch.generateIds(branches);
    for (String _branchId : branchIds) {
      final CohortRig cohort = new CohortRig(new CohortRig.Config() {{
        ledgerFactory = globalLedgerFactory;
        channelFactory = globalChannelFactory;
        branchId = _branchId;
      }});
      rigs.add(cohort);
    }
    
    final MonitorRig monitor = new MonitorRig(new MonitorRig.Config() {{
      ledgerFactory = globalLedgerFactory;
      channelFactory = globalChannelFactory;
    }});
    rigs.add(monitor);
    
    initiator.run();
  }
}
