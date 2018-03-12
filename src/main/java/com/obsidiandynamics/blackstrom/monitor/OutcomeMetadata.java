package com.obsidiandynamics.blackstrom.monitor;

import org.apache.commons.lang3.builder.*;

public final class OutcomeMetadata {
  private final long proposalTimestamp;

  public OutcomeMetadata(long proposalTimestamp) {
    this.proposalTimestamp = proposalTimestamp;
  }

  public long getProposalTimestamp() {
    return proposalTimestamp;
  }
  
  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(proposalTimestamp).toHashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj instanceof OutcomeMetadata) {
      final OutcomeMetadata that = (OutcomeMetadata) obj;
      return new EqualsBuilder().append(proposalTimestamp, that.proposalTimestamp).isEquals();
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return OutcomeMetadata.class.getSimpleName() + " [proposalTimestamp=" + proposalTimestamp + "]";
  }
}
