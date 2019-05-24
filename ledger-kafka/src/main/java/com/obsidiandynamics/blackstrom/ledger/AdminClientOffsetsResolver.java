package com.obsidiandynamics.blackstrom.ledger;

import static com.obsidiandynamics.func.Functions.*;

import java.util.*;
import java.util.concurrent.*;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;

import com.obsidiandynamics.zerolog.*;

/**
 *  Resolves offsets using a Kafka {@link AdminClient}.
 */
public final class AdminClientOffsetsResolver implements ConsumerGroupOffsetsResolver {
  private static final int LIST_OFFSETS_TIMEOUT = 10_000;
  
  private Zlg zlg = Zlg.forDeclaringClass().get();
  
  private final AdminClient adminClient;
  
  public AdminClientOffsetsResolver(AdminClient adminClient) {
    this.adminClient = mustExist(adminClient, "Admin client cannot be null");
  }
  
  AdminClientOffsetsResolver withZlg(Zlg zlg) {
    this.zlg = mustExist(zlg, "Zlg cannot be null");
    return this;
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> resolve(String groupId, Collection<TopicPartition> topicPartitions) {
    mustExist(groupId, "Group ID cannot be null");
    mustExist(topicPartitions, "Topic partitions cannot be null");

    final var opts = new ListConsumerGroupOffsetsOptions()
        .topicPartitions(List.copyOf(topicPartitions))
        .timeoutMs(LIST_OFFSETS_TIMEOUT);
    final var listConsumerGroupOffsets = adminClient.listConsumerGroupOffsets(groupId, opts);
    try {
      return listConsumerGroupOffsets.partitionsToOffsetAndMetadata().get();
    } catch (ExecutionException e) {
      // suppress IllegalArgumentException, as Kafka may construct OffsetAndMetadata with a negative offset 
      if (! (e.getCause() instanceof IllegalArgumentException)) {
        zlg.w("Error resolving offsets for consumer group %s: %s", z -> z.arg(groupId).arg(e));
      }
      return Collections.emptyMap();
    } catch (InterruptedException e) {
      return Collections.emptyMap();
    }
  }
}
