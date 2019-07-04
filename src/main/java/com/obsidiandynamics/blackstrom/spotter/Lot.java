package com.obsidiandynamics.blackstrom.spotter;

public final class Lot {
  private final int shard;
  
  private long offset = -1;
  
  private long lastAdvancedTime = System.currentTimeMillis();
  
  private boolean logPrinted;
  
  public Lot(int shard) {
    this.shard = shard;
  }
  
  public int getShard() {
    return shard;
  }
  
  public long getOffset() {
    return offset;
  }
  
  boolean tryAdvance(long offset) {
    if (offset > this.offset) {
      this.offset = offset;
      lastAdvancedTime = System.currentTimeMillis();
      logPrinted = false;
      return true;
    } else {
      return false;
    }
  }
  
  void setLogPrinted() {
    logPrinted = true;
  }
  
  boolean isLogPrinted() {
    return logPrinted;
  }
  
  public long getLastAdvancedTime() {
    return lastAdvancedTime;
  }
}
