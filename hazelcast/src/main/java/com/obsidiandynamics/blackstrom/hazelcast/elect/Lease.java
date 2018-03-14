package com.obsidiandynamics.blackstrom.hazelcast.elect;

import java.nio.*;
import java.util.*;

import org.apache.commons.lang3.builder.*;

public final class Lease {
  static final Lease VACANT = new Lease(null, 0);
  
  private final UUID candidateId;
  
  private final long expiry;

  Lease(UUID candidateId, long expiry) {
    this.candidateId = candidateId;
    this.expiry = expiry;
  }
  
  public boolean isVacant() {
    return candidateId == null;
  }
  
  public boolean isHeldBy(UUID candidateId) {
    return candidateId.equals(this.candidateId);
  }
  
  public boolean isHeldByAndCurrent(UUID candidateId) {
    return isHeldBy(candidateId) && isCurrent();
  }
  
  public UUID getCandidateId() {
    return candidateId;
  }

  public long getExpiry() {
    return expiry;
  }
  
  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(candidateId).append(expiry).hashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj instanceof Lease) {
      final Lease that = (Lease) obj;
      return new EqualsBuilder().append(candidateId, that.candidateId).append(expiry, that.expiry).isEquals();
    } else {
      return false;
    }
  }
  
  public boolean isCurrent() {
    return expiry != 0 && System.currentTimeMillis() <= expiry;
  }

  @Override
  public String toString() {
    return Lease.class.getSimpleName() + " [candidateId=" + candidateId + ", expiry=" + expiry + "]";
  }
  
  byte[] pack() {
    final ByteBuffer buf = ByteBuffer.allocate(24);
    buf.putLong(candidateId.getMostSignificantBits());
    buf.putLong(candidateId.getLeastSignificantBits());
    buf.putLong(expiry);
    return buf.array();
  }
  
  static Lease unpack(byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final UUID candidateId = new UUID(buf.getLong(), buf.getLong());
    final long expiry = buf.getLong();
    return new Lease(candidateId, expiry);
  }
}
