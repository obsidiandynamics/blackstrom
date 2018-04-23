package com.obsidiandynamics.blackstrom.rig;

import static com.obsidiandynamics.props.Props.*;
import static org.junit.Assert.*;

import java.util.*;
import java.util.concurrent.*;

import org.apache.kafka.clients.admin.*;
import org.jgroups.*;

import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.jackdaw.*;
import com.obsidiandynamics.jgroups.*;
import com.obsidiandynamics.props.*;
import com.obsidiandynamics.threads.*;
import com.obsidiandynamics.zerolog.*;

public final class KafkaRig {
  enum Encoding {
    JSON {
      @Override String getTopic() {
        return TestTopic.of(KafkaRig.class, "json", JacksonMessageCodec.ENCODING_VERSION, cluster);
      }
      
      @Override MessageCodec getCodec() {
        return new JacksonMessageCodec(true, new JacksonBankExpansion());
      }
    },     
    KRYO {
      @Override String getTopic() {
        return TestTopic.of(KafkaRig.class, "kryo", KryoMessageCodec.ENCODING_VERSION, cluster);
      }
      
      @Override MessageCodec getCodec() {
        return new KryoMessageCodec(true, new KryoBankExpansion());
      }
    };  
    
    abstract String getTopic();
    abstract MessageCodec getCodec();
  }
  
  private static final Properties base = new Properties(System.getProperties());
  
  private static final String cluster = getOrSet(base, "rig.cluster", String::valueOf, "rig");
  private static final String bootstrapServers = getOrSet(base, "bootstrap.servers", String::valueOf, "localhost:9092");
  private static final boolean producerAsync = getOrSet(base, "rig.producer.async", Boolean::valueOf, false);
  private static final boolean consumerAsync = getOrSet(base, "rig.consumer.async", Boolean::valueOf, true);
  private static final Encoding encoding = getOrSet(base, "rig.encoding", Encoding::valueOf, Encoding.KRYO);
  
  private static final Zlg zlg = Zlg.forDeclaringClass().get();
  
  private static final KafkaClusterConfig config = new KafkaClusterConfig()
      .withBootstrapServers(bootstrapServers);

  private static void before() throws InterruptedException, ExecutionException, TimeoutException {
    try (KafkaAdmin admin = KafkaAdmin.forConfig(config, AdminClient::create)) {
      admin.describeCluster(KafkaTimeouts.CLUSTER_AWAIT);
      admin.ensureExists(TestTopic.newOf(encoding.getTopic()), KafkaTimeouts.TOPIC_CREATE);
    }
  }
  
  private static void printProps(Properties props) {
    zlg.i("Rig properties:");
    PropsFormat.printStandard(zlg::i, props, 25, "rig.");
  }
  
  private static Ledger createLedger() {
    return new KafkaLedger(new KafkaLedgerConfig()
                           .withProducerPipeConfig(new ProducerPipeConfig().withAsync(producerAsync))
                           .withConsumerPipeConfig(new ConsumerPipeConfig().withAsync(consumerAsync))
                           .withKafka(new KafkaCluster<>(config))
                           .withTopic(encoding.getTopic())
                           .withCodec(encoding.getCodec())
                           .withPrintConfig(true));
  }
  
  private static JChannel createChannel() throws Exception {
    return Group.newUdpChannel(null);
  }
  
  static {
    zlg.t("Trace enabled");
  }
  
  public static final class Initiator {
    public static void main(String[] args) throws Exception {
      final Properties props = new Properties(base);
      final long _runs = getOrSet(props, "rig.runs", Long::valueOf, 1_000_000L);
      final int _backlogTarget = getOrSet(props, "rig.backlog", Integer::valueOf, 10_000);
      final int cycles = getOrSet(props, "rig.cycles", Integer::valueOf, 1);
      printProps(props);
      
      before();
      
      for (int cycle = 0; cycle < cycles; cycle++) {
        if (cycles != 1) {
          final int cycleNumber = cycle + 1;
          zlg.i("——");
          zlg.i("Cycle #%,d/%,d", z -> z.arg(cycleNumber).arg(cycles));
        }
        
        new InitiatorRig.Config() {{
          zlg = KafkaRig.zlg;
          ledgerFactory = KafkaRig::createLedger;
          channelFactory = KafkaRig::createChannel;
          clusterName = KafkaRig.cluster;
          runs = _runs;
          backlogTarget = _backlogTarget;
        }}.create().run();
      }
    }
  }
  
  public static final class Cohort {
    public static void main(String[] args) throws Exception {
      final Properties props = new Properties(base);
      final String _branchId = getOrSet(props, "rig.branch.id", String::valueOf, null);
      assertNotNull("rig.branch.id not set", _branchId);
      printProps(props);
      before();
      
      new CohortRig.Config() {{
        zlg = KafkaRig.zlg;
        ledgerFactory = KafkaRig::createLedger;
        channelFactory = KafkaRig::createChannel;
        clusterName = KafkaRig.cluster;
        branchId = _branchId;
      }}.create();
      
      Threads.sleep(Integer.MAX_VALUE);
    }
  }
  
  public static final class Monitor {
    public static void main(String[] args) throws Exception {
      before();
      
      new MonitorRig.Config() {{
        zlg = KafkaRig.zlg;
        ledgerFactory = KafkaRig::createLedger;
        channelFactory = KafkaRig::createChannel;
        clusterName = KafkaRig.cluster;
      }}.create();
      
      Threads.sleep(Integer.MAX_VALUE);
    }
  }
}
