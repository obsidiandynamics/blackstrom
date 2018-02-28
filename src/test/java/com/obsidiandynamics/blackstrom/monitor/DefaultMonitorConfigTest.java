package com.obsidiandynamics.blackstrom.monitor;

import static org.junit.Assert.*;

import java.io.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;
import com.obsidiandynamics.yconf.*;

public final class DefaultMonitorConfigTest {
  @Test
  public void testConfig() throws IOException {
    final DefaultMonitorConfig config = new MappingContext()
        .withParser(new SnakeyamlParser())
        .fromStream(DefaultMonitorConfig.class.getClassLoader().getResourceAsStream("defaultmonitor.conf"))
        .map(DefaultMonitorConfig.class);
    assertEquals("test", config.getGroupId());
    assertEquals(1, config.getGCInterval());
    assertEquals(2, config.getOutcomeLifetime());
    assertEquals(3, config.getTimeoutInterval());
    assertTrue(config.isTrackingEnabled());
    assertTrue(config.isMetadataEnabled());
  }
  
  @Test
  public void testToString() {
    Assertions.assertToStringOverride(new DefaultMonitorConfig());
  }
}
