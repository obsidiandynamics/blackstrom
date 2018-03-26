package com.obsidiandynamics.blackstrom.hazelcast.queue;

import static org.junit.Assert.*;

import org.junit.*;
import org.slf4j.*;

import com.obsidiandynamics.assertion.*;
import com.obsidiandynamics.blackstrom.hazelcast.elect.*;

public final class SubscriberConfigTest {
  @Test
  public void testConfig() {
    final ElectionConfig electionConfig = new ElectionConfig();
    final ErrorHandler errorHandler = ErrorHandler.nop();
    final String group = "group";
    final InitialOffsetScheme initialOffsetScheme = InitialOffsetScheme.EARLIEST;
    final Logger log = LoggerFactory.getLogger(SubscriberConfigTest.class);
    final StreamConfig streamConfig = new StreamConfig();
    final double staleReadSafetyMargin = 0.5;
    
    final SubscriberConfig config = new SubscriberConfig()
        .withElectionConfig(electionConfig)
        .withErrorHandler(errorHandler)
        .withGroup(group)
        .withInitialOffsetScheme(initialOffsetScheme)
        .withLog(log)
        .withStreamConfig(streamConfig)
        .withStaleReadSafetyMargin(staleReadSafetyMargin);
    assertEquals(electionConfig, config.getElectionConfig());
    assertEquals(errorHandler, config.getErrorHandler());
    assertEquals(group, config.getGroup());
    assertEquals(initialOffsetScheme, config.getInitialOffsetScheme());
    assertEquals(log, config.getLog());
    assertEquals(streamConfig, config.getStreamConfig());
    assertEquals(staleReadSafetyMargin, config.getStaleReadSafetyMargin(), Double.MIN_VALUE);
    
    Assertions.assertToStringOverride(config);
  }
}