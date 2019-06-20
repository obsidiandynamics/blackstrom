package com.obsidiandynamics.blackstrom.run;

import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.manifold.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.format.*;
import com.obsidiandynamics.jackdaw.*;
import com.obsidiandynamics.random.*;
import com.obsidiandynamics.threads.*;

public final class RunKafkaLedgerProducer {
  public static void main(String[] args) {
    final var kafkaConfig = new KafkaClusterConfig().withBootstrapServers(TestKafka.bootstrapServers());
    final var kafka = new KafkaCluster<String, Message>(kafkaConfig);
    final var ledgerConfig = new KafkaLedgerConfig()
        .withTopic("run-kafka-ledger")
        .withCodec(new JacksonMessageCodec())
        .withKafka(kafka);
    final var ledger = new KafkaLedger(ledgerConfig);
    try (var manifold = Manifold.builder()
        .withLedger(ledger)
        .build()
        .closeable()) {
      
      for (;;) {
        final var message = new Notice(Binary.toHex(Randomness.nextBytes(8)), "hello").withShardKey("key");
        ledger.append(message, (messageId, error) -> {
          System.out.println("Published " + messageId);
        });
        Threads.sleep(500);
      }
    }
  }
}
