package com.obsidiandynamics.blackstrom.run;

import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.cohort.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.manifold.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.jackdaw.*;
import com.obsidiandynamics.threads.*;

public final class RunKafkaLedgerConsumer {
  public static void main(String[] args) {
    final var groupId = "run-kafka-ledger-consumer";
    final var kafkaConfig = new KafkaClusterConfig().withBootstrapServers("localhost:9092");
    final var kafka = new KafkaCluster<String, Message>(kafkaConfig);
    final var ledgerConfig = new KafkaLedgerConfig()
        .withTopic("run-kafka-ledger")
        .withCodec(new JacksonMessageCodec())
        .withKafka(kafka);
    final var ledger = new KafkaLedger(ledgerConfig);
    try (var manifold = Manifold.builder()
        .withLedger(ledger)
        .withFactor(LambdaCohort.builder()
                    .withGroupId(groupId)
                    .onNotice((context, notice) -> {
                      System.out.println("Consumed " + notice.getMessageId());
                      context.beginAndConfirm(notice);
                    })
                    .onUnhandled(MessageContext::beginAndConfirm)
                    .build())
        .build()
        .closeable()) {
      Threads.sleep(Long.MAX_VALUE);
    }
  }
}
