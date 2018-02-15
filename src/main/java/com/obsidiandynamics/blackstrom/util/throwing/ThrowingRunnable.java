package com.obsidiandynamics.blackstrom.util.throwing;

@FunctionalInterface
public interface ThrowingRunnable {
  void run() throws Exception;
  
  static void noOp() {}
}