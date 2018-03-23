package com.obsidiandynamics.blackstrom.hazelcast.queue;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.junit.*;

import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.obsidiandynamics.blackstrom.hazelcast.queue.store.*;
import com.obsidiandynamics.blackstrom.util.props.*;

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
    
    final StreamConfig streamConfig = new StreamConfig()
        .withName("stream")
        .withAsyncReplicas(3)
        .withSyncReplicas(2)
        .withHeapCapacity(100)
        .withStoreFactoryClass(HeapRingbufferStore.Factory.class);
    streamConfig.getStoreFactoryProps().put("foo", "bar");
    
    StreamHelper.getRingbuffer(instance, streamConfig);
    verify(instance).getConfig();
    
    final RingbufferConfig r = config.getRingbufferConfig(QNamespace.HAZELQ_STREAM.qualify(streamConfig.getName()));
    assertEquals(streamConfig.getAsyncReplicas(), r.getAsyncBackupCount());
    assertEquals(streamConfig.getSyncReplicas(), r.getBackupCount());
    assertEquals(streamConfig.getHeapCapacity(), r.getCapacity());
    assertEquals(streamConfig.getStoreFactoryClass().getName(), r.getRingbufferStoreConfig().getFactoryClassName());
    assertEquals(new PropsBuilder().with("foo", "bar").build(), r.getRingbufferStoreConfig().getProperties());
  }
}
