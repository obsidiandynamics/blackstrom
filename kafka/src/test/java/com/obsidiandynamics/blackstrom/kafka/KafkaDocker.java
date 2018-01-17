package com.obsidiandynamics.blackstrom.kafka;

import com.obsidiandynamics.dockercompose.*;
import com.obsidiandynamics.indigo.util.*;
import com.obsidiandynamics.shell.*;

public final class KafkaDocker {
  private static final String PROJECT = "blackstrom";
  
  private static final DockerCompose COMPOSE = new DockerCompose()
      .withProject(PROJECT)
      .withEcho(true)
      .withSink(TestSupport.LOG_STREAM::append)
      .withComposeFile("stack/docker-compose.yaml");
  
  private KafkaDocker() {}
  
  public static boolean isRunning() {
    final StringBuilder sink = new StringBuilder();
    BourneUtils.run("docker ps | grep \"" + PROJECT + "_kafka\" | wc -l", null, false, sink::append);
    return Integer.parseInt(sink.toString().trim()) >= 1;
  }
  
  public static void start() throws Exception {
    COMPOSE.checkInstalled();
    TestSupport.LOG_STREAM.format("Starting Kafka stack...\n");
    if (isRunning()) {
      TestSupport.LOG_STREAM.format("Kafka already running\n");
      return;
    }
    
    final long took = TestSupport.tookThrowing(() -> {
      COMPOSE.up();
    });
    TestSupport.LOG_STREAM.format("took %,d ms\n", took);
  }
  
  public static void stop() throws Exception {
    TestSupport.LOG_STREAM.format("Stopping Kafka stack...\n");
    final long took = TestSupport.tookThrowing(() -> {
      COMPOSE.stop(1);
      COMPOSE.down(true);
    });
    TestSupport.LOG_STREAM.format("took %,d ms\n", took);
  }
}
