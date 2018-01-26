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
  
  private BalancedLedgerHub hub;
  
  @Override
  protected void startup() {
    hub = new BalancedLedgerHub(2, StickyShardAssignment::new, ArrayListAccumulator.factory(100));
  }
  
  @Override
  protected void shutdown() {
    hub.dispose();
  }
  
  @Override
  protected Ledger createLedgerImpl() {
    return hub.connect();
  }
}
