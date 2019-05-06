package com.obsidiandynamics.blackstrom.codec;

/**
 *  Signifies that the implementing object represents an intermediate serialized form
 *  that can be further unpacked — reconstructing the original object graph — given the 
 *  appropriate {@link Unpacker} implementation.
 */
public interface PackedForm {
  int hashCode();
  
  boolean equals(Object obj);
}
