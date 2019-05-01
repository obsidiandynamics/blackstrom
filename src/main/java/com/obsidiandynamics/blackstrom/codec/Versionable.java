package com.obsidiandynamics.blackstrom.codec;

import static com.obsidiandynamics.func.Functions.*;

import java.util.*;

import com.obsidiandynamics.func.*;

public final class Versionable {
  private final ContentHandle handle;
  private final PackedForm packed;
  private final Object content;
  
  Versionable(ContentHandle handle, PackedForm packed, Object content) {
    this.handle = mustExist(handle, "Content handle cannot be null");
    mustBeTrue(packed != null ^ content != null, illegalArgument("Either the packed form or the unpacked content must be specified"));
    this.packed = packed;
    this.content = content;
  }

  public ContentHandle getHandle() {
    return handle;
  }

  public PackedForm getPacked() {
    return packed;
  }

  public <T> T getContent() {
    return Classes.cast(content);
  }
  
  @Override
  public int hashCode() {
    final var prime = 31;
    var result = 1;
    result = prime * result + Objects.hashCode(handle);
    result = prime * result + Objects.hashCode(packed);
    result = prime * result + Objects.hashCode(content);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj instanceof Versionable) {
      final var that = (Versionable) obj;
      return Objects.equals(handle, that.handle) && Objects.equals(packed, that.packed) && Objects.equals(content, that.content);
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return Versionable.class.getSimpleName() + " [handle=" + handle + ", packed=" + packed + ", content=" + content + "]";
  }
}
