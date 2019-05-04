package com.obsidiandynamics.blackstrom.codec;

import static com.obsidiandynamics.func.Functions.*;

import java.util.*;

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
