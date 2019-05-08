package com.obsidiandynamics.blackstrom.codec;

/**
 *  A stand-in for a non-existent content object, in lieu of the constraint that 
 *  variants cannot capture a {@code null}.
 */
public final class Nil {
  private static final Nil INSTANCE = new Nil();
  
  public static Nil getInstance() { return INSTANCE; }
  
  private Nil() {}
  
  @Override
  public int hashCode() {
    return Nil.class.hashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    return this == obj || obj instanceof Nil;
  }
  
  @Override
  public String toString() {
    return Nil.class.getSimpleName();
  }
}
