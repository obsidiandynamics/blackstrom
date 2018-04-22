package com.obsidiandynamics.blackstrom.ledger;

import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.hazelq.*;
import com.obsidiandynamics.yconf.*;
import com.obsidiandynamics.zerolog.*;

@Y
public final class HazelQLedgerConfig {
  @YInject
  private MessageCodec codec;
  
  @YInject
  private Zlg zlg = Zlg.forDeclaringClass().get();
  
  @YInject
  private StreamConfig streamConfig = new StreamConfig();
  
  @YInject
  private ElectionConfig electionConfig = new ElectionConfig();
  
  @YInject
  private int pollIntervalMillis = 100;

  MessageCodec getCodec() {
    return codec;
  }

  public HazelQLedgerConfig withCodec(MessageCodec codec) {
    this.codec = codec;
    return this;
  }

  Zlg getZlg() {
    return zlg;
  }

  public HazelQLedgerConfig withZlg(Zlg zlg) {
    this.zlg = zlg;
    return this;
  }

  StreamConfig getStreamConfig() {
    return streamConfig;
  }

  public HazelQLedgerConfig withStreamConfig(StreamConfig streamConfig) {
    this.streamConfig = streamConfig;
    return this;
  }
  
  ElectionConfig getElectionConfig() {
    return electionConfig;
  }
  
  public HazelQLedgerConfig withElectionConfig(ElectionConfig electionConfig) {
    this.electionConfig = electionConfig;
    return this;
  }

  int getPollInterval() {
    return pollIntervalMillis;
  }

  public HazelQLedgerConfig withPollInterval(int pollIntervalMillis) {
    this.pollIntervalMillis = pollIntervalMillis;
    return this;
  }

  @Override
  public String toString() {
    return HazelQLedgerConfig.class.getSimpleName() + " [codec=" + codec + ", streamConfig=" + streamConfig
        + ", electionConfig=" + electionConfig + ", pollInterval=" + pollIntervalMillis + "]";
  }
}
