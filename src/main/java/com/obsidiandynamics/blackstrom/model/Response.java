package com.obsidiandynamics.blackstrom.model;

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
  public String toString() {
    return "Respose [cohort=" + cohort + ", pledge=" + pledge + ", metadata=" + metadata + "]";
  }
}
