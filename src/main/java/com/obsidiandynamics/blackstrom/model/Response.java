package com.obsidiandynamics.blackstrom.model;

import com.obsidiandynamics.blackstrom.util.*;

public final class Response {
  private final String cohort;
  private final Plea plea;
  private final Object metadata;
  
  public Response(String cohort, Plea plea, Object metadata) {
    this.cohort = cohort;
    this.plea = plea;
    this.metadata = metadata;
  }

  public String getCohort() {
    return cohort;
  }

  public Plea getPlea() {
    return plea;
  }

  public <T> T getMetadata() {
    return Cast.from(metadata);
  }

  @Override
  public String toString() {
    return "Respose [cohort=" + cohort + ", plea=" + plea + ", metadata=" + metadata + "]";
  }
}
