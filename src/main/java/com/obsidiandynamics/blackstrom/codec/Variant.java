package com.obsidiandynamics.blackstrom.codec;

/**
 *  A container that captures one or more (depending on the implementation) variations 
 *  of a serialized content object and provides a mechanism for conditionally mapping 
 *  the encapsulated content to a concrete Java class upon deserialization. The
 *  mapping process is assisted by an appropriately configured 
 *  {@link ContentMapper} instance, acting as a registry of version mappings. <p>
 *  
 *  The mapping will only proceed if the {@link ContentMapper} is configured with 
 *  version mappings corresponding to at least one of the content version(s) captured 
 *  within the {@link Variant} container, otherwise the mapping process will silently
 *  return a {@code null}.
 *  
 *  @see ContentMapper
 *  @see UniVariant
 *  @see MultiVariant
 */
public interface Variant {
  /**
   *  Maps the captured content to its corresponding concrete Java type and reconstructs
   *  the complete object graph using the version mappings defined in the supplied 
   *  {@link ContentMapper}. If the content could not be mapped to the Java type (because
   *  no suitable mapping could be located), a {@code null} is returned.
   *  
   *  @param mapper The mapper to aid in resolving the concrete Java type.
   *  @return The reconstructed object, or {@code null} if the captured content version 
   *          is not supported by the {@code mapper}.
   */
  Object map(ContentMapper mapper);
}
