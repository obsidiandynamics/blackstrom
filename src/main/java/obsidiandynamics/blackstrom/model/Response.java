package obsidiandynamics.blackstrom.model;

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

  public Object getMetadata() {
    return metadata;
  }

  @Override
  public String toString() {
    return "Answer [cohort=" + cohort + ", plea=" + plea + ", metadata=" + metadata + "]";
  }
}
