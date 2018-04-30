package com.obsidiandynamics.blackstrom.rig;

import static com.obsidiandynamics.props.Props.*;
import static org.junit.Assert.*;

import java.util.*;

import org.jgroups.*;

import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.hazelq.*;
import com.obsidiandynamics.jgroups.*;
import com.obsidiandynamics.props.*;
import com.obsidiandynamics.threads.*;
import com.obsidiandynamics.zerolog.*;

public final class HazelQRig {
  private static final Zlg zlg = Zlg.forDeclaringClass().get();
  
  enum Encoding {
    JSON {
      @Override String getStream() {
        return "json";
      }
      
      @Override MessageCodec getCodec() {
        return new JacksonMessageCodec(true, new JacksonBankExpansion());
      }
    },     
    KRYO {
      @Override String getStream() {
        return "kryo";
      }
      
      @Override MessageCodec getCodec() {
        return new KryoMessageCodec(true, new KryoBankExpansion());
      }
    };  
    
    abstract String getStream();
    abstract MessageCodec getCodec();
  }
  
  private static final Properties base = new Properties(System.getProperties());
  private static final String cluster = getOrSet(base, "rig.cluster", String::valueOf, "rig");
  private static final int hazelcastPartitions = getOrSet(base, "rig.hazelcast.partitions", Integer::valueOf, 7);
  private static final boolean hazelcastDebugMigrations = getOrSet(base, "rig.hazelcast.debug.migrations", Boolean::parseBoolean, false);
  private static final boolean hazelcastCleanShutdown = getOrSet(base, "rig.hazelcast.clean.shutdown", Boolean::parseBoolean, true);
  private static final Encoding encoding = getOrSet(base, "rig.encoding", Encoding::valueOf, Encoding.KRYO);
  
  private static void printProps(Properties props) {
    zlg.c("Rig properties:");
    PropsFormat.printStandard(zlg::c, props, 25, "rig.");
  }
  
  private static HazelcastInstance instance;
  
  private static final Object instanceLock = new Object();
  
  private static void configureHazelcastInstanceAsync() {
    new Thread(HazelQRig::configureHazelcastInstance, "hazelcast-configure").start();
  }
  
  private static void configureHazelcastInstance() {
    HazelcastZlgBridge.install();
    
    synchronized (instanceLock) {
      shutdownHazelcastInstance();
      zlg.i("Creating Hazelcast instance");
      final Config config = new Config()
          .setProperty("hazelcast.shutdownhook.enabled", "false")
          .setProperty("hazelcast.max.no.heartbeat.seconds", String.valueOf(5))
          .setProperty("hazelcast.partition.count", String.valueOf(hazelcastPartitions))
          .setGroupConfig(new GroupConfig()
                          .setName(cluster)
                          .setPassword(""))
          .setNetworkConfig(new NetworkConfig()
                            .setJoin(new JoinConfig()
                                     .setMulticastConfig(new MulticastConfig()
                                                         .setMulticastTimeoutSeconds(1))
                                     .setTcpIpConfig(new TcpIpConfig()
                                                     .setEnabled(false))));
      instance = GridProvider.getInstance().createInstance(config);
      if (hazelcastDebugMigrations) {
        instance.getPartitionService().addMigrationListener(new MigrationListener() {
          @Override public void migrationStarted(MigrationEvent migrationEvent) {}
  
          @Override public void migrationCompleted(MigrationEvent migrationEvent) {
            zlg.i("Migration compeleted %s", z -> z.arg(migrationEvent));
          }
  
          @Override public void migrationFailed(MigrationEvent migrationEvent) {
            zlg.i("Migration failed %s", z -> z.arg(migrationEvent));
          }
        });
      }
      zlg.i("Hazelcast instance ready");
    }
  }
  
  private static void shutdownHazelcastInstance() {
    synchronized (instanceLock) {
      if (instance != null) {
        zlg.i("Shutting down existing Hazelcast instance");
        instance.shutdown();
        instance = null;
      }
    }
  }
  
  private static Ledger createLedger() {
    synchronized (instanceLock) {
      final StreamConfig streamConfig = new StreamConfig()
          .withName(encoding.getStream())
          .withSyncReplicas(1)
          .withAsyncReplicas(0)
          .withHeapCapacity(100_000);
      return new HazelQLedger(instance, 
                              new HazelQLedgerConfig()
                              .withElectionConfig(new ElectionConfig()
                                                  .withScavengeInterval(1000))
                              .withStreamConfig(streamConfig)
                              .withPollInterval(1000)
                              .withCodec(encoding.getCodec()));
    }
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
      final long _runs = getOrSet(props, "rig.runs", Long::valueOf, 100_000L);
      final int _backlogTarget = getOrSet(props, "rig.backlog", Integer::valueOf, 10_000);
      final int cycles = getOrSet(props, "rig.cycles", Integer::valueOf, 1);
      final int _progressIntervalMillis = getOrSet(props, "rig.progress.interval.ms", Integer::valueOf, 2_000);
      printProps(props);
      configureHazelcastInstanceAsync();
      
      for (int cycle = 0; cycle < cycles; cycle++) {
        if (cycles != 1) {
          final int cycleNumber = cycle + 1;
          zlg.i("——");
          zlg.i("Cycle #%,d/%,d", z -> z.arg(cycleNumber).arg(cycles));
        }
        
        new InitiatorRig.Config() {{
          zlg = HazelQRig.zlg;
          ledgerFactory = HazelQRig::createLedger;
          channelFactory = HazelQRig::createChannel;
          clusterName = HazelQRig.cluster;
          runs = _runs;
          backlogTarget = _backlogTarget;
          groupAnnounceWaitMillis = 10_000;
          progressIntervalMillis = _progressIntervalMillis;
        }}.create().run();
      }
      if (hazelcastCleanShutdown) shutdownHazelcastInstance();
    }
  }
  
  public static final class Cohort {
    public static void main(String[] args) throws Exception {
      final Properties props = new Properties(base);
      final String _branchId = getOrSet(props, "rig.branch.id", String::valueOf, null);
      assertNotNull("rig.branch.id not set", _branchId);
      printProps(props);
      configureHazelcastInstanceAsync();
      
      final CohortRig cohortRig = new CohortRig.Config() {{
        zlg = HazelQRig.zlg;
        ledgerFactory = HazelQRig::createLedger;
        channelFactory = HazelQRig::createChannel;
        clusterName = HazelQRig.cluster;
        branchId = _branchId;
      }}.create();
      if (hazelcastCleanShutdown) Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        cohortRig.dispose();
        shutdownHazelcastInstance();
      }));
      
      Threads.sleep(Integer.MAX_VALUE);
    }
  }
  
  public static final class Monitor {
    public static void main(String[] args) throws Exception {
      final Properties props = new Properties(base);
      printProps(props);
      configureHazelcastInstanceAsync();
      final MonitorRig monitorRig = new MonitorRig.Config() {{
        zlg = HazelQRig.zlg;
        ledgerFactory = HazelQRig::createLedger;
        channelFactory = HazelQRig::createChannel;
        clusterName = HazelQRig.cluster;
      }}.create();
      if (hazelcastCleanShutdown) Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        monitorRig.dispose();
        shutdownHazelcastInstance();
      }));
      
      Threads.sleep(Integer.MAX_VALUE);
    }
  }
}
