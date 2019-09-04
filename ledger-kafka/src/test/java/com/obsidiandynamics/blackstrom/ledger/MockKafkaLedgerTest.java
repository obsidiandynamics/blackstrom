package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;

import org.junit.runner.*;
import org.junit.runners.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.jackdaw.*;
import com.obsidiandynamics.junit.*;

@RunWith(Parameterized.class)
public final class MockKafkaLedgerTest extends AbstractLedgerTest {
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return TestCycle.timesQuietly(1);
  }
  
  @Override
  protected Timesert getWait() {
    return Wait.MEDIUM;
  }
  
  @Override
  protected Ledger createLedger() {
    return MockKafkaLedger.create(config -> config
                                  .withMaxConsumerPipeYields(1)
                                  .withConsumerPipeConfig(new ConsumerPipeConfig()
                                                          .withBacklogBatches(1)));
  }
}
