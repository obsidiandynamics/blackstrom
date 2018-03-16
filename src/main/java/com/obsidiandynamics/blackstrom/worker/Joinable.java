package com.obsidiandynamics.blackstrom.worker;

import java.util.*;

/**
 *  Definition of a concurrent entity that can be waited upon to
 *  have completed carrying out its work.
 */
@FunctionalInterface
public interface Joinable {
  public static Joinable nop() { return timeoutMillis -> true; }
  
  /**
   *  Waits until this concurrent entity terminates.
   *  
   *  @param timeoutMillis The time to wait. {@code 0} means wait forever.
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
   *  @param timeoutMillis The time to wait. {@code 0} means wait forever.
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
  
  static boolean joinAll(long timeoutMillis, Joinable... joinables) throws InterruptedException {
    return joinAll(timeoutMillis, Arrays.asList(joinables));
  }
 
  static boolean joinAll(long timeoutMillis, Collection<? extends Joinable> joinables) throws InterruptedException {
    final long deadline = timeoutMillis != 0 ? System.currentTimeMillis() + timeoutMillis : Long.MAX_VALUE;
    for (Joinable joinable : joinables) {
      final long remainingMillis = deadline - System.currentTimeMillis();
      if (remainingMillis > 0) {
        final boolean joined = joinable.join(timeoutMillis);
        if (! joined) return false;
      } else {
        return false;
      }
    }
    return true;
  }
}
