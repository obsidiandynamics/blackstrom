package com.obsidiandynamics.blackstrom.codec;

import static com.obsidiandynamics.func.Functions.*;

import java.util.*;

/**
 *  A container for multiple {@link UniVariant}s, by convention arranged in the descending
 *  order of the content version. When mapping to the Java class, the variants are enumerated
 *  in a fallback manner, trying the first variant then advancing to the next, until either
 *  all variants are exhausted (yielding a {@code null}) or a supported content type and
 *  version is located (yielding the reconstituted object). 
 */
public final class MultiVariant implements Variant {
  private final UniVariant[] variants;

  public MultiVariant(UniVariant... variants) {
    this.variants = mustExist(variants, "Variants cannot be null");
    mustBeGreater(variants.length, 0, illegalArgument("Variants cannot be empty"));
  }

  public final UniVariant[] getVariants() {
    return variants;
  }
  
  @Override
  public <C> C map(ContentMapper mapper) {
    mustExist(mapper, "Content mapper cannot be null");
    for (var nested : variants) {
      final var mapped = nested.<C>map(mapper);
      if (mapped != null) {
        return mapped;
      }
    }
    return null;
  }
  
  @Override
  public int hashCode() {
    return Arrays.hashCode(variants);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj instanceof MultiVariant) {
      final var that = (MultiVariant) obj;
      return Arrays.equals(variants, that.variants);
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return MultiVariant.class.getSimpleName() + " [variants=" + Arrays.toString(variants) + "]";
  }
}
