package com.obsidiandynamics.blackstrom.model;

import org.apache.commons.lang3.builder.*;

import com.obsidiandynamics.blackstrom.util.*;

public final class Response {
  private final String cohort;
  private final Intent intent;
  private final Object metadata;
  
  public Response(String cohort, Intent intent, Object metadata) {
    this.cohort = cohort;
    this.intent = intent;
    this.metadata = metadata;
  }

  public String getCohort() {
    return cohort;
  }

  public Intent getIntent() {
    return intent;
  }

  public <T> T getMetadata() {
    return Cast.from(metadata);
  }
  
  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(cohort)
        .append(intent)
        .append(metadata)
        .toHashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj instanceof Response) {
      final Response that = (Response) obj;
      return new EqualsBuilder()
          .append(cohort, that.cohort)
          .append(intent, that.intent)
          .append(metadata, that.metadata)
          .isEquals();
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return Response.class.getSimpleName() + " [cohort=" + cohort + ", intent=" + intent + ", metadata=" + metadata + "]";
  }
}
