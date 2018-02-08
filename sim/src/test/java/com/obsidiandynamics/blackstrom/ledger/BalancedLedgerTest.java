package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;

import org.junit.runner.*;
import org.junit.runners.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.junit.*;

@RunWith(Parameterized.class)
public final class BalancedLedgerTest extends AbstractLedgerTest {  
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
    return new BalancedLedgerHub(2, StickyShardAssignment::new, ArrayListAccumulator.factory(1_000, 1_000))
        .connectDetached();
  }
  
  public static void main(String[] args) {
    Testmark.enable().withOptions(Scale.by(4));
    JUnitCore.runClasses(BalancedLedgerTest.class);
  }
}
