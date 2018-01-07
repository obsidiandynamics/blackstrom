package com.obsidiandynamics.blackstrom.model;

import org.apache.commons.lang3.builder.*;

import com.obsidiandynamics.blackstrom.util.*;

public final class Response {
  private final String cohort;
  private final Pledge pledge;
  private final Object metadata;
  
  public Response(String cohort, Pledge pledge, Object metadata) {
    this.cohort = cohort;
    this.pledge = pledge;
    this.metadata = metadata;
  }

  public String getCohort() {
    return cohort;
  }

  public Pledge getPledge() {
    return pledge;
  }

  public <T> T getMetadata() {
    return Cast.from(metadata);
  }
  
  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(cohort)
        .append(pledge)
        .append(metadata)
        .toHashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj instanceof Response) {
      final Response other = (Response) obj;
      return new EqualsBuilder()
          .append(cohort, other.cohort)
          .append(pledge, other.pledge)
          .append(metadata, other.metadata)
          .isEquals();
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return Response.class.getSimpleName() + " [cohort=" + cohort + ", pledge=" + pledge + ", metadata=" + metadata + "]";
  }
}
