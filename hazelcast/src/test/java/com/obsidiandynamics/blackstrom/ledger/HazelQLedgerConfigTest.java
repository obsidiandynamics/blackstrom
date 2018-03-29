package com.obsidiandynamics.blackstrom.ledger;

import static org.junit.Assert.*;

import org.junit.*;
import org.slf4j.*;

import com.obsidiandynamics.assertion.*;
import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.hazelcast.elect.*;
import com.obsidiandynamics.blackstrom.hazelcast.queue.*;

public final class HazelQLedgerConfigTest {
  @Test
  public void testConfig() {
    final MessageCodec codec = new KryoMessageCodec(false);
    final ElectionConfig electionConfig = new ElectionConfig();
    final Logger log = LoggerFactory.getLogger(HazelQLedgerConfigTest.class);
    final int pollIntervalMillis = 500;
    final StreamConfig streamConfig = new StreamConfig();
    final HazelQLedgerConfig config = new HazelQLedgerConfig()
        .withCodec(codec)
        .withElectionConfig(electionConfig)
        .withLog(log)
        .withPollInterval(pollIntervalMillis)
        .withStreamConfig(streamConfig);
    assertEquals(codec, config.getCodec());
    assertEquals(electionConfig, config.getElectionConfig());
    assertEquals(log, config.getLog());
    assertEquals(pollIntervalMillis, config.getPollInterval());
    assertEquals(streamConfig, config.getStreamConfig());
  }

  @Test
  public void testToString() {
    Assertions.assertToStringOverride(new HazelQLedgerConfig());
  }
}
