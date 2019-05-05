package com.obsidiandynamics.blackstrom.codec;

import static com.obsidiandynamics.func.Functions.*;

import java.util.*;

public final class KryoPackedForm implements PackedForm {
  private final byte[] bytes;

  public KryoPackedForm(byte[] bytes) {
    this.bytes = mustExist(bytes, "Bytes array cannot be null");
  }

  public final byte[] getBytes() {
    return bytes;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(bytes);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj instanceof KryoPackedForm) {
      final var that = (KryoPackedForm) obj;
      return Arrays.equals(bytes, that.bytes);
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return KryoPackedForm.class.getSimpleName() + " [bytes=" + Arrays.toString(bytes) + "]";
  }
}
