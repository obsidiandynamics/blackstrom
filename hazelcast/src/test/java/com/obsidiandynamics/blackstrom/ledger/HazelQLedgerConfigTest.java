package com.obsidiandynamics.blackstrom.ledger;

import static org.junit.Assert.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;
import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.hazelq.*;
import com.obsidiandynamics.zerolog.*;

public final class HazelQLedgerConfigTest {
  @Test
  public void testConfig() {
    final MessageCodec codec = new KryoMessageCodec(false);
    final ElectionConfig electionConfig = new ElectionConfig();
    final Zlg zlg = Zlg.forDeclaringClass().get();
    final int pollIntervalMillis = 500;
    final StreamConfig streamConfig = new StreamConfig();
    final HazelQLedgerConfig config = new HazelQLedgerConfig()
        .withCodec(codec)
        .withElectionConfig(electionConfig)
        .withZlg(zlg)
        .withPollInterval(pollIntervalMillis)
        .withStreamConfig(streamConfig);
    assertEquals(codec, config.getCodec());
    assertEquals(electionConfig, config.getElectionConfig());
    assertEquals(zlg, config.getZlg());
    assertEquals(pollIntervalMillis, config.getPollInterval());
    assertEquals(streamConfig, config.getStreamConfig());
  }

  @Test
  public void testToString() {
    Assertions.assertToStringOverride(new HazelQLedgerConfig());
  }
}
