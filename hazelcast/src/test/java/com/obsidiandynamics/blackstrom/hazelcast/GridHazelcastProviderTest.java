package com.obsidiandynamics.blackstrom.hazelcast;

import static org.junit.Assert.*;

import org.junit.*;

import com.hazelcast.config.*;
import com.hazelcast.core.*;

public final class GridHazelcastProviderTest {
  private HazelcastProvider provider;
  
  @Before
  public void before() {
    provider = GridHazelcastProvider.getInstance();
  }
  
  @After
  public void after() {
    if (provider != null) provider.shutdownAll();
  }
  
  @Test
  public void testCreate() {
    final Config config = new Config()
        .setProperty("hazelcast.shutdownhook.enabled", "false")
        .setProperty("hazelcast.logging.type", "none");
    
    final MulticastConfig multicastConfig = new MulticastConfig()
        .setEnabled(false);
    
    final TcpIpConfig tcpIpConfig = new TcpIpConfig()
        .setEnabled(false);
    
    config.setNetworkConfig(new NetworkConfig().setJoin(new JoinConfig()
                                                        .setMulticastConfig(multicastConfig)
                                                        .setTcpIpConfig(tcpIpConfig)));
    final HazelcastInstance instance = GridHazelcastProvider.getInstance().createInstance(config);
    assertNotNull(instance);
  }
}
