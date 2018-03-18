package com.obsidiandynamics.blackstrom.worker;

@FunctionalInterface
public interface Terminable {
  Joinable terminate();
}
