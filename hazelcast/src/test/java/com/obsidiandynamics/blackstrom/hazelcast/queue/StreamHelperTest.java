package com.obsidiandynamics.blackstrom.hazelcast.queue;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.junit.*;

import com.hazelcast.config.*;
import com.hazelcast.core.*;

public final class StreamHelperTest {
  @Test
  public void testIsNotNull() {
    assertTrue(StreamHelper.isNotNull(new byte[0]));
    assertFalse(StreamHelper.isNotNull(null));
  }

  @Test
  public void testGetRingbuffer() {
    final HazelcastInstance instance = mock(HazelcastInstance.class);
    final Config config = new Config();
    when(instance.getConfig()).thenReturn(config);
    
    final RingbufferStoreConfig ringbufferStoreConfig = new RingbufferStoreConfig()
        .setEnabled(true)
        .setClassName("TestClass");
    final StreamConfig streamConfig = new StreamConfig()
        .withName("stream")
        .withAsyncReplicas(3)
        .withSyncReplicas(2)
        .withHeapCapacity(100)
        .withRingbufferStoreConfig(ringbufferStoreConfig);
    
    StreamHelper.getRingbuffer(instance, streamConfig);
    verify(instance).getConfig();
    
    final RingbufferConfig r = config.getRingbufferConfig(QNamespace.HAZELQ_STREAM.qualify(streamConfig.getName()));
    assertEquals(streamConfig.getAsyncReplicas(), r.getAsyncBackupCount());
    assertEquals(streamConfig.getSyncReplicas(), r.getBackupCount());
    assertEquals(streamConfig.getHeapCapacity(), r.getCapacity());
    assertEquals(streamConfig.getRingbufferStoreConfig(), r.getRingbufferStoreConfig());
  }
}
