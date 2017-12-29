package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;

import org.junit.runner.*;
import org.junit.runners.*;

import com.obsidiandynamics.junit.*;

@RunWith(Parameterized.class)
public final class MultiNodeQueueLedgerTest extends AbstractLedgerTest {
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return TestCycle.timesQuietly(2);
  }
  
  @Override
  protected Ledger createLedgerImpl() {
    return new MultiNodeQueueLedger();
  }
}
