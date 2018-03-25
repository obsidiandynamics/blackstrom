package com.obsidiandynamics.blackstrom.rig;

import static com.obsidiandynamics.indigo.util.PropertyUtils.*;
import static org.junit.Assert.*;

import java.util.*;

import org.jgroups.*;
import org.slf4j.*;

import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.group.*;
import com.obsidiandynamics.blackstrom.hazelcast.*;
import com.obsidiandynamics.blackstrom.hazelcast.elect.*;
import com.obsidiandynamics.blackstrom.hazelcast.queue.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.util.props.*;
import com.obsidiandynamics.indigo.util.*;

public final class HazelQRig {
  private static final Properties base = new Properties(System.getProperties());
  
  private static final String cluster = getOrSet(base, "rig.cluster", String::valueOf, "rig");
  
  private static final Logger log = LoggerFactory.getLogger(HazelQRig.class);
  
  private static void printProps(Properties props) {
    log.info("Rig properties:");
    PropsFormat.printStandard(log::info, props, 25, "rig.");
  }
  
  private static HazelcastInstance instance;
  
  private static final Object instanceLock = new Object();
  
  private static void configureHazelcastInstanceAsync() {
    new Thread(HazelQRig::configureHazelcastInstance, "hazelcast-configure").start();
  }
  
  private static void configureHazelcastInstance() {
    synchronized (instanceLock) {
      shutdownHazelcastInstance();
      log.info("Creating Hazelcast instance");
      final Config config = new Config()
          .setProperty("hazelcast.logging.type", "none")
          .setProperty("hazelcast.shutdownhook.enabled", "false")
          .setProperty("hazelcast.max.no.heartbeat.seconds", String.valueOf(5))
          .setNetworkConfig(new NetworkConfig()
                            .setJoin(new JoinConfig()
                                     .setMulticastConfig(new MulticastConfig()
                                                         .setMulticastTimeoutSeconds(1))
                                     .setTcpIpConfig(new TcpIpConfig()
                                                     .setEnabled(false))));
      instance = GridHazelcastProvider.getInstance().createInstance(config);
      log.info("Hazelcast instance ready");
    }
  }
  
  private static void shutdownHazelcastInstance() {
    synchronized (instanceLock) {
      if (instance != null) {
        log.info("Shutting down existing Hazelcast instance");
        instance.shutdown();
        instance = null;
      }
    }
  }
  
  private static Ledger createLedger() {
    synchronized (instanceLock) {
      final StreamConfig streamConfig = new StreamConfig()
          .withName("rig")
          .withSyncReplicas(1)
          .withAsyncReplicas(0)
          .withHeapCapacity(100_000);
      return new HazelQLedger(instance, 
                              new HazelQLedgerConfig()
                              .withElectionConfig(new ElectionConfig()
                                                  .withScavengeInterval(100))
                              .withStreamConfig(streamConfig)
                              .withCodec(new KryoMessageCodec(true, new KryoBankExpansion())));
    }
  }
  
  private static JChannel createChannel() throws Exception {
    return Group.newUdpChannel(null);
  }
  
  public static final class Initiator {
    public static void main(String[] args) throws Exception {
      final Properties props = new Properties(base);
      final long _runs = getOrSet(props, "rig.runs", Long::valueOf, 100_000L);
      final int _backlogTarget = getOrSet(props, "rig.backlog", Integer::valueOf, 10_000);
      final int cycles = getOrSet(props, "rig.cycles", Integer::valueOf, 1);
      printProps(props);
      configureHazelcastInstanceAsync();
      
      for (int cycle = 0; cycle < cycles; cycle++) {
        if (cycles != 1) {
          log.info("——");
          log.info("Cycle #{}/{}", cycle + 1, cycles);
        }
        
        new InitiatorRig.Config() {{
          log = HazelQRig.log;
          ledgerFactory = HazelQRig::createLedger;
          channelFactory = HazelQRig::createChannel;
          clusterName = HazelQRig.cluster;
          runs = _runs;
          backlogTarget = _backlogTarget;
          groupAnnounceWaitMillis = 10_000;
        }}.create().run();
      }
      shutdownHazelcastInstance();
    }
  }
  
  public static final class Cohort {
    public static void main(String[] args) throws Exception {
      final Properties props = new Properties(base);
      final String _branchId = getOrSet(props, "rig.branch.id", String::valueOf, null);
      assertNotNull("rig.branch.id not set", _branchId);
      printProps(props);
      configureHazelcastInstanceAsync();
      
      new CohortRig.Config() {{
        log = HazelQRig.log;
        ledgerFactory = HazelQRig::createLedger;
        channelFactory = HazelQRig::createChannel;
        clusterName = HazelQRig.cluster;
        branchId = _branchId;
      }}.create();
      
      TestSupport.sleep(Integer.MAX_VALUE);
    }
  }
  
  public static final class Monitor {
    public static void main(String[] args) throws Exception {
      configureHazelcastInstanceAsync();
      new MonitorRig.Config() {{
        log = HazelQRig.log;
        ledgerFactory = HazelQRig::createLedger;
        channelFactory = HazelQRig::createChannel;
        clusterName = HazelQRig.cluster;
      }}.create();
      
      TestSupport.sleep(Integer.MAX_VALUE);
    }
  }
}
