package com.obsidiandynamics.blackstrom.rig;

import static org.jgroups.Message.Flag.*;
import static org.junit.Assert.*;

import java.util.function.*;

import org.jgroups.*;
import org.slf4j.*;

import com.obsidiandynamics.blackstrom.*;
import com.obsidiandynamics.blackstrom.bank.*;
import com.obsidiandynamics.blackstrom.group.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.manifold.*;
import com.obsidiandynamics.blackstrom.monitor.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.blackstrom.util.select.*;
import com.obsidiandynamics.blackstrom.util.throwing.*;

public final class MonitorRig implements Disposable {
  private static final Logger LOG = LoggerFactory.getLogger(MonitorRig.class);
  
  public static class Config {
    Supplier<Ledger> ledgerFactory;
    CheckedSupplier<JChannel, Exception> channelFactory;
    String clusterName = "rig";
    
    void validate() {
      assertNotNull(ledgerFactory);
      assertNotNull(channelFactory);
    }
  }
  
  private final Config config;
  
  private final Group group;
  
  private volatile Manifold manifold;
  
  public MonitorRig(Config config) throws Exception {
    config.validate();
    this.config = config;
    
    group = new Group(config.channelFactory.get());
    group.withHandler(this::onMessage);
    connect();
  }
  
  private void connect() throws Exception {
    LOG.info("Joining cluster '{}'", config.clusterName);
    group.connect(config.clusterName);
  }
  
  private void onMessage(JChannel chan, org.jgroups.Message m) throws Exception {
    Select.from(m.getObject()).checked()
    .whenInstanceOf(AnnouncePacket.class).then(p -> {
      build();
      chan.send(new Message(m.getSrc(), Ack.of(p)).setFlag(DONT_BUNDLE));
    });
  }
  
  private void build() {
    LOG.info("Building manifold");
    cleanupExisting();

    final boolean trackingEnabled = false;
    final Monitor monitor = new DefaultMonitor(new DefaultMonitorOptions().withTrackingEnabled(trackingEnabled));
    final Ledger ledger = config.ledgerFactory.get();
    manifold = Manifold.builder()
        .withLedger(ledger)
        .withFactors(monitor)
        .build();
  }
  
  private void cleanupExisting() {
    if (manifold != null) {
      manifold.dispose();
      manifold = null;
    }
  }
  
  @Override
  public void dispose() {
    group.close();
    cleanupExisting();
  }
}
