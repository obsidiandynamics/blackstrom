package com.obsidiandynamics.blackstrom.ledger;

import static org.junit.Assert.*;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.stream.*;

import org.junit.*;
import org.junit.runner.*;
import org.junit.runners.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;
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
  
  @Test
  public void testConfig() {
    final MultiNodeQueueLedger.Config config = new MultiNodeQueueLedger.Config();
    config.withMaxYields(10);
    assertEquals(10, config.maxYields);
  }
  
  @Test
  public void testDebugMessageCount() {
    // test coverage of debug messages -- no further assertions
    
    final AtomicInteger received = new AtomicInteger();
    final int messages = 10;
    useLedger(new MultiNodeQueueLedger(new MultiNodeQueueLedger.Config().withDebugMessageCounts(messages)));
    ledger.attach((NullGroupMessageHandler) (c, m) -> received.incrementAndGet());
    IntStream.range(0, messages).boxed().forEach(i -> ledger.append(new Proposal(i + "", new String[0], null, 0)));
    
    wait.until(() -> assertEquals(10, received.get()));
  }
  
  public static void main(String[] args) {
    Testmark.enable().withOptions(Scale.by(8));
    JUnitCore.runClasses(MultiNodeQueueLedgerTest.class);
  }
}
