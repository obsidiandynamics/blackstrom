package com.obsidiandynamics.blackstrom.hazelcast.queue;

public final class RawRecord {
  public static final long UNASSIGNED_OFFSET = -1;
  
  private long offset = UNASSIGNED_OFFSET;
  
  private final byte[] data;
  
  public RawRecord(byte[] data) {
    this.data = data;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public byte[] getData() {
    return data;
  }

  @Override
  public String toString() {
    return RawRecord.class.getSimpleName() + " [offset=" + offset + ", data.length=" + data.length + "]";
  }
}
