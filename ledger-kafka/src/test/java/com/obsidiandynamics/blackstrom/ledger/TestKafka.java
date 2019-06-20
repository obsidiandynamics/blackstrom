package com.obsidiandynamics.blackstrom.ledger;

import static org.junit.Assert.*;

import java.io.*;

import com.obsidiandynamics.blackstrom.*;
import com.obsidiandynamics.func.*;
import com.obsidiandynamics.io.*;
import com.obsidiandynamics.jackdaw.*;
import com.obsidiandynamics.props.*;

public final class TestKafka {
  private static final String KAFKA_HOST = Props.get("blackstrom.kafka.host", String::valueOf, "localhost");
  
  private static final int KAFKA_PORT = Props.get("blackstrom.kafka.port", Integer::parseInt, 9092);
  
  private static final int PORT_CHECK_TIMEOUT = 10_000;
  
  public static String bootstrapServers() {
    return KAFKA_HOST + ":" + KAFKA_PORT;
  }
  
  public static void start() throws IOException, InterruptedException {
    try (var lock = DockerFSLock.getRoot().acquire("kafka")) {
      if (! Sockets.isRemotePortListening(KAFKA_HOST, KAFKA_PORT, PORT_CHECK_TIMEOUT)) {
        if (KAFKA_HOST.equals("localhost")) {
          final var kafkaDocker = new KafkaDocker().withComposeFile("stack/docker-compose.yaml").withPort(KAFKA_PORT);
          Exceptions.wrap(kafkaDocker::start, RuntimeException::new);
        } else {
          fail(String.format("Kafka not available on %s:%d", KAFKA_HOST, KAFKA_PORT));
        }
      }
    }
  }
}
