package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;

import org.junit.runner.*;
import org.junit.runners.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.junit.*;

@RunWith(Parameterized.class)
public final class BalancedLedgerGroupTest extends AbstractGroupLedgerTest {  
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return TestCycle.timesQuietly(1);
  }
  
  @Override
  protected Timesert getWait() {
    return Wait.SHORT;
  }
  
  @Override
  protected Ledger createLedger() {
    return new BalancedLedgerHub(2, StickyShardAssignment::new, ArrayListAccumulator.factory(2, 10))
        .connectDetached();
  }
}
