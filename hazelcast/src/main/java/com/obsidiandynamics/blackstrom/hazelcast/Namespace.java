package com.obsidiandynamics.blackstrom.hazelcast;

public final class Namespace {
  private final String namespace;

  public Namespace(String namespace) {
    this.namespace = namespace;
  }
  
  public String qualify(String objectName) {
    return namespace + "::" + objectName;
  }
}
