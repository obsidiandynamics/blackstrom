package com.obsidiandynamics.blackstrom.rig;

import static com.obsidiandynamics.indigo.util.PropertyUtils.*;
import static org.junit.Assert.*;

import org.jgroups.*;

import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.group.*;
import com.obsidiandynamics.blackstrom.kafka.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.indigo.util.*;

public final class KafkaRig {
  private static final String clusterName = get("rig.cluster.name", String::valueOf, "rig");
  private static final String bootstrapServers = get("rig.bootstrap.servers", String::valueOf, "localhost:9092");
  
  private static final KafkaClusterConfig config = new KafkaClusterConfig()
      .withBootstrapServers(bootstrapServers);

  private static final String topic = 
      TestTopic.of(KafkaRig.class, "kryo", KryoMessageCodec.ENCODING_VERSION, clusterName);
  
  private static void before() {
    //TODO create topic
  }
  
  private static Ledger createLedger() {
    return new KafkaLedger(new KafkaLedgerConfig()
                           .withKafka(new KafkaCluster<>(config))
                           .withTopic(topic)
                           .withCodec(new KryoMessageCodec(true, new KryoBankExpansion())));
  }
  
  private static JChannel createChannel() throws Exception {
    return Group.newUdpChannel(null);
  }
  
  public static final class Initiator {
    public static void main(String[] args) throws Exception {
      final long _runs = get("rig.runs", Long::valueOf, 1_000_000L);
      before();
      
      new InitiatorRig.Config() {{
        ledgerFactory = KafkaRig::createLedger;
        channelFactory = KafkaRig::createChannel;
        clusterName = KafkaRig.clusterName;
        runs = _runs;
      }}.create().run();
    }
  }
  
  public static final class Cohort {
    public static void main(String[] args) throws Exception {
      final String _branchId = get("rig.branch.id", String::valueOf, null);
      assertNotNull(_branchId);
      before();
      
      new CohortRig.Config() {{
        ledgerFactory = KafkaRig::createLedger;
        channelFactory = KafkaRig::createChannel;
        clusterName = KafkaRig.clusterName;
        branchId = _branchId;
      }}.create();
      
      TestSupport.sleep(Integer.MAX_VALUE);
    }
  }
  
  public static final class Monitor {
    public static void main(String[] args) throws Exception {
      before();
      
      new MonitorRig.Config() {{
        ledgerFactory = KafkaRig::createLedger;
        channelFactory = KafkaRig::createChannel;
        clusterName = KafkaRig.clusterName;
      }}.create();
      
      TestSupport.sleep(Integer.MAX_VALUE);
    }
  }
}
