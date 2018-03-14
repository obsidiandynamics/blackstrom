package com.obsidiandynamics.blackstrom.worker;

import com.obsidiandynamics.concat.*;

public final class WorkerOptions {
  private String name;
  
  private boolean daemon;
  
  private int priority = 5;

  public String getName() {
    return name;
  }

  public WorkerOptions withName(String name) {
    this.name = name;
    return this;
  }
  
  public WorkerOptions withName(Class<?> cls, Object... nameFrags) {
    final String name = new Concat()
        .append(cls.getSimpleName())
        .when(nameFrags.length > 0).append(new Concat().append("-").appendArray("-", nameFrags))
        .toString();
    return withName(name);
  }

  public boolean isDaemon() {
    return daemon;
  }

  public WorkerOptions withDaemon(boolean daemon) {
    this.daemon = daemon;
    return this;
  }

  public int getPriority() {
    return priority;
  }

  public WorkerOptions withPriority(int priority) {
    this.priority = priority;
    return this;
  }

  @Override
  public String toString() {
    return WorkerOptions.class.getSimpleName() + " [name=" + name + ", daemon=" + daemon + ", priority=" + priority + "]";
  }
}
