package com.obsidiandynamics.blackstrom.bank;

import java.util.*;

import org.junit.runner.*;
import org.junit.runners.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.junit.*;

@RunWith(Parameterized.class)
public final class BalancedLedgerBankTransferTest extends AbstractBankTransferTest {
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return TestCycle.timesQuietly(1);
  }
  
  @Override
  protected Ledger createLedger() {
    return new BalancedLedgerHub(1, StickyShardAssignment::new, ArrayListAccumulator.factory(1_000)).connect();
  }

  @Override
  protected Timesert getWait() {
    return Wait.SHORT;
  }
  
  public static void main(String[] args) {
    Testmark.enable();
    JUnitCore.runClasses(BalancedLedgerBankTransferTest.class);
  }
}
