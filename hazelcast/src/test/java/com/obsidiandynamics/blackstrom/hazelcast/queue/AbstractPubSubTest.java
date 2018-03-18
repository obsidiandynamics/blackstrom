package com.obsidiandynamics.blackstrom.hazelcast.queue;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.*;
import java.util.stream.*;

import org.junit.*;
import org.junit.runners.*;
import org.slf4j.*;

import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.hazelcast.*;
import com.obsidiandynamics.blackstrom.hazelcast.queue.Receiver.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.blackstrom.worker.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public abstract class AbstractPubSubTest {
  protected HazelcastProvider defaultProvider;
  
  protected final Set<HazelcastInstance> instances = new HashSet<>();
  
  protected final Set<Terminable> terminables = new HashSet<>();

  protected final Timesert await = Wait.SHORT;
  
  @Before
  public void before() {
    defaultProvider = new MockHazelcastProvider();
  }
  
  @After
  public void after() {
    final Set<Joinable> joinables = terminables.stream()
        .map(t -> t.terminate()).collect(Collectors.toSet());
    joinables.forEach(s -> s.joinQuietly());
    instances.forEach(h -> h.shutdown());
  }
  
  protected final HazelcastInstance newInstance() {
    return newInstance(defaultProvider);
  }
  
  protected final HazelcastInstance newInstance(HazelcastProvider provider) {
    final Config config = new Config()
        .setProperty("hazelcast.logging.type", "none");
    return register(provider.createInstance(config), instances);
  }
  
  protected final DefaultPublisher configurePublisher(PublisherConfig config) {
    return configurePublisher(newInstance(), config);
  }
  
  protected final DefaultPublisher configurePublisher(HazelcastInstance instance, PublisherConfig config) {
    return register(Publisher.createDefault(instance, config), terminables);
  }
  
  protected final DefaultSubscriber configureSubscriber(SubscriberConfig config) {
    return configureSubscriber(newInstance(), config);
  }
  
  protected final DefaultSubscriber configureSubscriber(HazelcastInstance instance, SubscriberConfig config) {
    return register(Subscriber.createDefault(instance, config), terminables);
  }
  
  protected final Receiver createReceiver(Subscriber subscriber, RecordHandler recordHandler, int pollTimeoutMillis) {
    return register(subscriber.createReceiver(recordHandler, pollTimeoutMillis), terminables);
  }
  
  protected static final <T> T register(T item, Collection<? super T> container) {
    container.add(item);
    return item;
  }
  
  protected static final String randomGroup() {
    final UUID random = UUID.randomUUID();
    return "group-" + Long.toHexString(random.getMostSignificantBits() ^ random.getLeastSignificantBits());
  }
  
  protected static final ErrorHandler mockErrorHandler() {
    final ErrorHandler mock = mock(ErrorHandler.class);
    doAnswer(invocation -> {
      final String summary = invocation.getArgument(0);
      final Throwable error = invocation.getArgument(1);
      final Logger log = LoggerFactory.getLogger(AbstractPubSubTest.class);
      log.warn(summary, error);
      return null;
    }).when(mock).onError(any(), any());
    return mock;
  }
  
  protected static final void verifyNoError(ErrorHandler... mockErrorHandlers) {
    Arrays.stream(mockErrorHandlers).forEach(AbstractPubSubTest::verifyNoError);
  }
  
  protected static final void verifyNoError(ErrorHandler mockErrorHandler) {
    verify(mockErrorHandler, never()).onError(any(), any());
  }
}