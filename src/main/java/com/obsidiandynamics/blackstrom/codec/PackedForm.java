package com.obsidiandynamics.blackstrom.codec;

/**
 *  Marker interface signifying that the object represents an intermediate serialized form
 *  that can be unpacked, given the appropriate {@link Unpacker} implementation.
 */
public interface PackedForm {
  int hashCode();
  
  boolean equals(Object obj);
}
