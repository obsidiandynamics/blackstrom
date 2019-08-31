package com.obsidiandynamics.blackstrom.ledger;

import static org.junit.Assert.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;
import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.meteor.*;
import com.obsidiandynamics.zerolog.*;

public final class MeteorLedgerConfigTest {
  @Test
  public void testConfig() {
    final var codec = new KryoMessageCodec(false);
    final var electionConfig = new ElectionConfig();
    final var zlg = Zlg.forDeclaringClass().get();
    final var pollIntervalMillis = 500;
    final var streamConfig = new StreamConfig();
    final var config = new MeteorLedgerConfig()
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
    config.validate();
  }

  @Test
  public void testToString() {
    Assertions.assertToStringOverride(new MeteorLedgerConfig());
  }
}
