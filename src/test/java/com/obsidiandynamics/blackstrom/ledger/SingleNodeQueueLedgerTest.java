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
public final class SingleNodeQueueLedgerTest extends AbstractLedgerTest {  
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
    return new SingleNodeQueueLedger();
  }
  
  @Test
  public void testConfig() {
    final SingleNodeQueueLedger.Config config = new SingleNodeQueueLedger.Config();
    config.withMaxYields(10);
    assertEquals(10, config.maxYields);
  }
}
