package com.obsidiandynamics.blackstrom.ledger;

import static com.obsidiandynamics.func.Functions.*;

import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.meteor.*;
import com.obsidiandynamics.yconf.*;
import com.obsidiandynamics.zerolog.*;

@Y
public final class MeteorLedgerConfig {
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

  public void validate() {
    mustExist(codec, "Codec cannot be null");
    mustExist(zlg, "Zlg cannot be null");
    mustExist(streamConfig, "Stream config cannot be null");
    mustExist(electionConfig, "Election config cannot be null");
    mustBeGreaterOrEqual(pollIntervalMillis, 1, illegalArgument("Poll interval must be greater or equal to 1"));
  }

  MessageCodec getCodec() {
    return codec;
  }

  public MeteorLedgerConfig withCodec(MessageCodec codec) {
    this.codec = codec;
    return this;
  }

  Zlg getZlg() {
    return zlg;
  }

  public MeteorLedgerConfig withZlg(Zlg zlg) {
    this.zlg = zlg;
    return this;
  }

  StreamConfig getStreamConfig() {
    return streamConfig;
  }

  public MeteorLedgerConfig withStreamConfig(StreamConfig streamConfig) {
    this.streamConfig = streamConfig;
    return this;
  }
  
  ElectionConfig getElectionConfig() {
    return electionConfig;
  }
  
  public MeteorLedgerConfig withElectionConfig(ElectionConfig electionConfig) {
    this.electionConfig = electionConfig;
    return this;
  }

  int getPollInterval() {
    return pollIntervalMillis;
  }

  public MeteorLedgerConfig withPollInterval(int pollIntervalMillis) {
    this.pollIntervalMillis = pollIntervalMillis;
    return this;
  }

  @Override
  public String toString() {
    return MeteorLedgerConfig.class.getSimpleName() + " [codec=" + codec + ", streamConfig=" + streamConfig
        + ", electionConfig=" + electionConfig + ", pollInterval=" + pollIntervalMillis + "]";
  }
}
