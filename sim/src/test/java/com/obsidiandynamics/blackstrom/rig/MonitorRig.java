package com.obsidiandynamics.blackstrom.rig;

import static org.jgroups.Message.Flag.*;

import org.jgroups.*;

import com.obsidiandynamics.blackstrom.*;
import com.obsidiandynamics.blackstrom.group.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.manifold.*;
import com.obsidiandynamics.blackstrom.monitor.*;
import com.obsidiandynamics.blackstrom.util.select.*;

public final class MonitorRig implements Disposable {
  public static class Config extends RigConfig {
    @Override void validate() {
      super.validate();
    }
  }
  
  private final Config config;
  
  private final Group group;
  
  private String sandboxKey;
  
  private volatile Manifold manifold;
  
  public MonitorRig(Config config) throws Exception {
    config.validate();
    this.config = config;
    
    group = new Group(config.channelFactory.get());
    group.withHandler(this::onMessage);
    connect();
  }
  
  private void connect() throws Exception {
    config.log.info("Monitor: joining cluster '{}'", config.clusterName);
    group.connect(config.clusterName);
  }
  
  private void onMessage(JChannel chan, org.jgroups.Message m) throws Exception {
    Select.from(m.getObject()).checked()
    .whenInstanceOf(AnnouncePacket.class).then(p -> {
      build(p.getSandboxKey());
      chan.send(new Message(m.getSrc(), Ack.of(p)).setFlag(DONT_BUNDLE));
    });
  }
  
  private synchronized void build(String sandboxKey) {
    if (sandboxKey.equals(this.sandboxKey)) return;
    cleanupExisting();
    this.sandboxKey = sandboxKey;
    config.log.info("Monitor: building manifold with sandbox key {}", sandboxKey);

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
