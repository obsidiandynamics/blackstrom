package com.obsidiandynamics.blackstrom.bank;

import java.util.*;

import org.junit.runner.*;
import org.junit.runners.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.junit.*;

@RunWith(Parameterized.class)
public final class MultiNodeQueueLedgerBankTransferTest extends AbstractBankTransferTest {
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return TestCycle.timesQuietly(1);
  }
  
  @Override
  protected Ledger createLedger() {
    return new MultiNodeQueueLedger();
  }

  @Override
  protected Timesert getWait() {
    return Wait.SHORT;
  }
  
  public static void main(String[] args) {
    AbstractBankTransferTest.enableBenchmark();
    JUnitCore.runClasses(MultiNodeQueueLedgerBankTransferTest.class);
  }
}
