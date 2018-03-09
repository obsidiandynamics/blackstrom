package com.obsidiandynamics.blackstrom.monitor;

import static org.junit.Assert.*;

import java.io.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;
import com.obsidiandynamics.yconf.*;

public final class MonitorEngineConfigTest {
  @Test
  public void testConfig() throws IOException {
    final MonitorEngineConfig config = new MappingContext()
        .withParser(new SnakeyamlParser())
        .fromStream(MonitorEngineConfig.class.getClassLoader().getResourceAsStream("monitorengine.conf"))
        .map(MonitorEngineConfig.class);
    assertEquals(1, config.getGCInterval());
    assertEquals(2, config.getOutcomeLifetime());
    assertEquals(3, config.getTimeoutInterval());
    assertTrue(config.isTrackingEnabled());
    assertTrue(config.isMetadataEnabled());
  }
  
  @Test
  public void testToString() {
    Assertions.assertToStringOverride(new MonitorEngineConfig());
  }
}
