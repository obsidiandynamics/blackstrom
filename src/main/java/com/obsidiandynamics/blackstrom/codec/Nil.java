package com.obsidiandynamics.blackstrom.codec;

/**
 *  A stand-in for a non-existent content object, in lieu of the constraint that 
 *  variants cannot capture a {@code null}.
 */
public final class Nil {
  private static final Nil INSTANCE = new Nil();
  
  private static final ContentHandle CONTENT_HANDLE = new ContentHandle("std:nil", 1);
  
  private static final UniVariant CAPTURED = new UniVariant(CONTENT_HANDLE, null, INSTANCE);
  
  public static ContentHandle getContentHandle() { return CONTENT_HANDLE; }
  
  public static Nil getInstance() { return INSTANCE; }
  
  /**
   *  Obtains a pre-captured {@link UniVariant} containing a {@link Nil} content object. <p>
   *  
   *  This method is more efficient than capturing via {@link ContentMapper#capture(Object)} 
   *  as it does not require object allocation.
   *  
   *  @return A pre-captured {@link UniVariant} containing a {@link Nil} content object.
   */
  public static UniVariant capture() {
    return CAPTURED;
  }
  
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
