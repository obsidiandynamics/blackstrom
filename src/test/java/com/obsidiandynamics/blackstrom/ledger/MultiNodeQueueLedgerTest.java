package com.obsidiandynamics.blackstrom.ledger;

import static org.junit.Assert.*;

import java.util.*;

import org.junit.*;
import org.junit.runner.*;
import org.junit.runners.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.junit.*;

@RunWith(Parameterized.class)
public final class MultiNodeQueueLedgerTest extends AbstractLedgerTest {
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
    return new MultiNodeQueueLedger();
  }
  
  public static void main(String[] args) {
    Testmark.enable().withOptions(Scale.by(8));
    JUnitCore.runClasses(MultiNodeQueueLedgerTest.class);
  }
  
  @Test
  public void testConfig() {
    final MultiNodeQueueLedger.Config config = new MultiNodeQueueLedger.Config();
    config.withMaxYields(10);
    assertEquals(10, config.maxYields);
  }
}
