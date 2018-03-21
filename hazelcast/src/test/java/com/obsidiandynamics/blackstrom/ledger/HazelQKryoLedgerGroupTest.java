package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;

import org.junit.*;
import org.junit.runner.*;
import org.junit.runners.*;

import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.hazelcast.*;
import com.obsidiandynamics.blackstrom.hazelcast.elect.*;
import com.obsidiandynamics.blackstrom.hazelcast.queue.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.junit.*;

@RunWith(Parameterized.class)
public final class HazelQKryoLedgerGroupTest extends AbstractGroupLedgerTest {  
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return TestCycle.timesQuietly(1);
  }
  
  @Override
  protected Timesert getWait() {
    return Wait.SHORT;
  }
  
  private HazelcastInstance instance;
  
  @Before
  public void before() {
    final Config config = new Config()
        .setProperty("hazelcast.logging.type", "none");
    instance = new TestHazelcastProvider().createInstance(config);
  }
  
  @After
  public void after() {
    afterBase();
    if (instance != null) instance.getLifecycleService().terminate();
  }
  
  @Override
  protected Ledger createLedger() {
    final HazelQLedgerConfig config = new HazelQLedgerConfig()
        .withCodec(new KryoMessageCodec(true, new KryoBankExpansion()))
        .withStreamConfig(new StreamConfig()
                          .withName("stream")
                          .withHeapCapacity(100_000))
        .withElectionConfig(new ElectionConfig().withScavengeInterval(1));
    return new HazelQLedger(instance, config);
  }
  
  public static void main(String[] args) {
    Testmark.enable();
    JUnitCore.runClasses(HazelQKryoLedgerGroupTest.class);
  }
}
