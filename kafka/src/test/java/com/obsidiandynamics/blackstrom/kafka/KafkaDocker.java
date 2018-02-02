package com.obsidiandynamics.blackstrom.kafka;

import com.obsidiandynamics.dockercompose.*;
import com.obsidiandynamics.indigo.util.*;
import com.obsidiandynamics.shell.*;

public final class KafkaDocker {
  private static final String PROJECT = "blackstrom";
  private static final String PATH = "/usr/local/bin";
  private static final int GRACE_PERIOD_MILLIS = 10_000;
  
  private static final DockerCompose COMPOSE = new DockerCompose()
      .withShell(new BourneShell().withPath(PATH))
      .withProject(PROJECT)
      .withEcho(true)
      .withSink(TestSupport.LOG_STREAM::append)
      .withComposeFile("stack/docker-compose.yaml");
  
  private KafkaDocker() {}
  
  public static boolean isRunning() {
    final StringBuilder sink = new StringBuilder();
    BourneUtils.run("docker ps | grep \"kafka\" | wc -l", PATH, false, sink::append);
    return Integer.parseInt(sink.toString().trim()) >= 1;
  }
  
  public static void start() throws Exception {
    TestSupport.LOG_STREAM.format("Starting Kafka stack...\n");
    if (isRunning()) {
      TestSupport.LOG_STREAM.format("Kafka already running\n");
      return;
    }

    COMPOSE.checkInstalled();
    final long took = TestSupport.tookThrowing(COMPOSE::up);
    TestSupport.LOG_STREAM.format("took %,d ms (will wait a further %,d ms)\n", took, GRACE_PERIOD_MILLIS);
    TestSupport.sleep(GRACE_PERIOD_MILLIS);
  }
  
  public static void stop() throws Exception {
    TestSupport.LOG_STREAM.format("Stopping Kafka stack...\n");
    if (! isRunning()) {
      TestSupport.LOG_STREAM.format("Kafka already stopped\n");
      return;
    }
    
    final long took = TestSupport.tookThrowing(() -> {
      COMPOSE.stop(1);
      COMPOSE.down(true);
    });
    TestSupport.LOG_STREAM.format("took %,d ms\n", took);
  }
}
