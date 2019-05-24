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
import org.assertj.core.api.*;
import org.junit.*;

import com.obsidiandynamics.zerolog.*;

public final class AdminClientOffsetsResolverTest {
  @Test
  public void testResolve_noError() {
    final var adminClient = mock(AdminClient.class);
    final var logTarget = new MockLogTarget();
    final var resolver = new AdminClientOffsetsResolver(adminClient).withZlg(logTarget.logger());
    
    final var expectedOffsets = singletonMap(new TopicPartition("test", 0), new OffsetAndMetadata(100));
    final var result = new XListConsumerGroupOffsetsResult(KafkaFuture.completedFuture(expectedOffsets));
    when(adminClient.listConsumerGroupOffsets(any(), any())).thenReturn(result);
    
    final var resolved = resolver.resolve("testGroup", emptyList());
    assertEquals(expectedOffsets, resolved);
    logTarget.entries().assertCount(0);
  }
  
  private static <T> KafkaFuture<T> completeExceptionally(Throwable cause) {
    final var future = new KafkaFutureImpl<T>();
    future.completeExceptionally(cause);
    return future;
  }
  
  @Test
  public void testResolve_genericError() {
    final var adminClient = mock(AdminClient.class);
    final var logTarget = new MockLogTarget();
    final var resolver = new AdminClientOffsetsResolver(adminClient).withZlg(logTarget.logger());
    
    final var cause = new Exception("Simulated");
    final var result = new XListConsumerGroupOffsetsResult(completeExceptionally(cause));
    when(adminClient.listConsumerGroupOffsets(any(), any())).thenReturn(result);
    
    final var resolved = resolver.resolve("testGroup", emptyList());
    assertEquals(emptyMap(), resolved);
    logTarget.entries().assertCount(1);
    //TODO
//    logTarget.entries().forLevel(LogLevel.WARN).containing("Error resolving offsets for consumer group testGroup: " + cause).assertCount(1);
  }
}
