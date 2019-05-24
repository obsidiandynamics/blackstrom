package com.obsidiandynamics.blackstrom.ledger;

import static java.util.Collections.*;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.concurrent.*;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.internals.*;
import org.junit.*;

import com.obsidiandynamics.zerolog.*;

public final class AdminClientOffsetsResolverTest {
  private AdminClient adminClient;
  
  private MockLogTarget logTarget;
  
  private AdminClientOffsetsResolver resolver;
  
  @Before
  public void before() {
    adminClient = mock(AdminClient.class);
    logTarget = new MockLogTarget();
    resolver = new AdminClientOffsetsResolver(adminClient).withZlg(logTarget.logger());
  }
  
  private static <T> KafkaFuture<T> completeExceptionally(Throwable cause) {
    final var future = new KafkaFutureImpl<T>();
    future.completeExceptionally(cause);
    return future;
  }
  
  @Test
  public void testResolve_noError() {
    final var expectedOffsets = singletonMap(new TopicPartition("test", 0), new OffsetAndMetadata(100));
    final var result = new XListConsumerGroupOffsetsResult(KafkaFuture.completedFuture(expectedOffsets));
    when(adminClient.listConsumerGroupOffsets(any(), any())).thenReturn(result);
    
    final var resolved = resolver.resolve("testGroup", emptyList());
    assertEquals(expectedOffsets, resolved);
    logTarget.entries().assertCount(0);
  }
  
  @Test
  public void testResolve_genericExecutionException() {
    final var cause = new Exception("Simulated");
    final var result = new XListConsumerGroupOffsetsResult(completeExceptionally(cause));
    when(adminClient.listConsumerGroupOffsets(any(), any())).thenReturn(result);
    
    final var resolved = resolver.resolve("testGroup", emptyList());
    assertEquals(emptyMap(), resolved);
    logTarget.entries().assertCount(1);
    logTarget.entries().forLevel(LogLevel.WARN)
    .containing("Error resolving offsets for consumer group testGroup: " + new ExecutionException(cause))
    .assertCount(1);
  }
  
  @Test
  public void testResolve_illegalArgumentException() {
    final var cause = new IllegalArgumentException("Simulated");
    final var result = new XListConsumerGroupOffsetsResult(completeExceptionally(cause));
    when(adminClient.listConsumerGroupOffsets(any(), any())).thenReturn(result);
    
    final var resolved = resolver.resolve("testGroup", emptyList());
    assertEquals(emptyMap(), resolved);
    logTarget.entries().assertCount(0);
  }
  
  @Test
  public void testResolve_interruptedException() {
    final var cause = new InterruptedException("Simulated");
    final var result = new XListConsumerGroupOffsetsResult(completeExceptionally(cause));
    when(adminClient.listConsumerGroupOffsets(any(), any())).thenReturn(result);
    
    final var resolved = resolver.resolve("testGroup", emptyList());
    assertEquals(emptyMap(), resolved);
    logTarget.entries().assertCount(0);
  }
}
