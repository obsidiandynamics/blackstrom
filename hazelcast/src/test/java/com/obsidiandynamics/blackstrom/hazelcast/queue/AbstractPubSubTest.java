package com.obsidiandynamics.blackstrom.hazelcast.queue;

import java.util.*;
import java.util.stream.*;

import org.junit.*;

import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.hazelcast.*;
import com.obsidiandynamics.blackstrom.hazelcast.queue.Receiver.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.blackstrom.worker.*;

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
  
  protected final <T> T register(T item, Collection<? super T> container) {
    container.add(item);
    return item;
  }
}