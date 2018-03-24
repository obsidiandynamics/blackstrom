package com.obsidiandynamics.blackstrom.rig;

import static org.jgroups.Message.Flag.*;
import static org.junit.Assert.*;

import org.jgroups.*;

import com.obsidiandynamics.blackstrom.*;
import com.obsidiandynamics.blackstrom.bank.*;
import com.obsidiandynamics.blackstrom.group.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.manifold.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.blackstrom.util.select.*;

public final class CohortRig implements Disposable {
  public static class Config extends RigConfig {
    String branchId;
    long initialBalance = Integer.MAX_VALUE;
    boolean guided = true;
    
    @Override void validate() {
      super.validate();
      assertNotNull(branchId);
    }
    
    public CohortRig create() throws Exception { 
      return new CohortRig(this); 
    }
  }
  
  private final Config config;
  
  private final Group group;
  
  private String sandboxKey;
  
  private volatile Manifold manifold;
  
  private CohortRig(Config config) throws Exception {
    config.validate();
    this.config = config;
    
    group = new Group(config.channelFactory.get());
    group.withHandler(this::onMessage);
    connect();
  }
  
  private void connect() throws Exception {
    config.log.info("Cohort {}: joining cluster '{}'", config.branchId, config.clusterName);
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
    config.log.info("Cohort {}: building manifold with sandbox key {}", config.branchId, sandboxKey);

    final boolean idempotencyEnabled = false;
    final Sandbox sandbox = Sandbox.forKey(sandboxKey);
    final BankBranch branch = new BankBranch(config.branchId, config.initialBalance, idempotencyEnabled, sandbox::contains);
    final Ledger ledger = config.ledgerFactory.get();
    manifold = Manifold.builder()
        .withLedger(ledger)
        .withFactors(branch)
        .build();
    config.log.info("Cohort {}: manifold ready", config.branchId);
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
