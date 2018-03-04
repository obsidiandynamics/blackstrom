package com.obsidiandynamics.blackstrom.worker;

import java.util.*;

/**
 *  Definition of a concurrent entity that can be waited upon to
 *  have completed carrying out its work.
 */
public interface Joinable {
  /**
   *  Waits until this concurrent entity terminates.
   *  
   *  @param timeoutMillis The time to wait.
   *  @return True if this entity was terminated, false if the wait timed out.
   *  @throws InterruptedException If the thread is interrupted.
   */
  boolean join(long timeoutMillis) throws InterruptedException;
  
  /**
   *  Waits until this concurrent entity terminates.
   *  
   *  @throws InterruptedException If the thread is interrupted.
   */
  default void join() throws InterruptedException {
    join(0);
  }
  
  /**
   *  Waits until this concurrent entity terminates.<p>
   *  
   *  This variant suppresses an {@link InterruptedException} and will re-assert the interrupt 
   *  prior to returning.
   *  
   *  @param timeoutMillis The time to wait.
   *  @return True if this entity was terminated, false if the wait timed out.
   */
  default boolean joinQuietly(long timeoutMillis) {
    try {
      return join(timeoutMillis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  /**
   *  Waits until this concurrent entity terminates.<p>
   *  
   *  This variant suppresses an {@link InterruptedException} and will re-assert the interrupt 
   *  prior to returning.
   */
  default void joinQuietly() {
    joinQuietly(0);
  }
 
  static boolean joinAll(Collection<? extends Joinable> joinables, long timeoutMillis) throws InterruptedException {
    for (Joinable joinable : joinables) {
      final boolean joined = joinable.join(timeoutMillis);
      if (! joined) break;
    }
    return true;
  }
}
