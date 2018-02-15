package com.obsidiandynamics.blackstrom.kafka;

import java.io.*;
import java.net.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.dockercompose.*;
import com.obsidiandynamics.indigo.util.*;
import com.obsidiandynamics.shell.*;

public final class KafkaDocker {
  private static final String PROJECT = "blackstrom";
  private static final String PATH = "/usr/local/bin";
  private static final int BROKER_AWAIT_MILLIS = 600_000;
  
  private static final DockerCompose COMPOSE = new DockerCompose()
      .withShell(new BourneShell().withPath(PATH))
      .withProject(PROJECT)
      .withEcho(true)
      .withSink(TestSupport.LOG_STREAM::append)
      .withComposeFile("stack/docker-compose.yaml");
  
  private KafkaDocker() {}
  
  public static boolean isRunning() {
    return isRemotePortListening("localhost", 9092);
  }
  
  private static boolean isRemotePortListening(String host, int port) {
    try (final Socket s = new Socket(host, port)) {
      return true;
    } catch (IOException e) {
      return false;
    }
  }
  
  public static void start() throws Exception {
    TestSupport.LOG_STREAM.format("Starting Kafka stack...\n");
    if (isRunning()) {
      TestSupport.LOG_STREAM.format("Kafka already running\n");
      return;
    }

    COMPOSE.checkInstalled();
    final long tookStart = TestSupport.tookThrowing(COMPOSE::up);
    TestSupport.LOG_STREAM.format("took %,d ms (now waiting for broker to come up...)\n", tookStart);
    final long tookAwait = awaitBroker();
    TestSupport.LOG_STREAM.format("broker up, waited %,d ms\n", tookAwait);
  }
  
  private static long awaitBroker() {
    return TestSupport.took(() -> Timesert.wait(BROKER_AWAIT_MILLIS).untilTrue(KafkaDocker::isRunning));
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
